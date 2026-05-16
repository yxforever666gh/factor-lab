from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_family_generators import (
    read_json,
    make_task,
    build_window_task,
    build_recent_validation_task,
    build_medium_horizon_task,
    build_watchlist_candidate_task,
    build_fragile_candidate_task,
    build_stable_candidate_task,
    build_graveyard_task,
)
from factor_lab.research_learning import build_research_learning
from factor_lab.candidate_generator import build_candidate_generation_plan
from factor_lab.candidate_compiler import compile_candidate_generation_plan
from factor_lab.research_promoter import should_promote_research_paths
from factor_lab.exploration_budget import exploration_floor_context
from factor_lab.exploration_pools import (
    NEW_MECHANISM_POOL,
    OLD_SPACE_POOL,
    split_exploration_pool_budget,
)
from factor_lab.regime_awareness import build_regime_context

ROOT = Path(__file__).resolve().parents[2]
AUTONOMY_POLICY_PATH = ROOT / "configs" / "research_autonomy_policy.json"
EVIDENCE_POLICY_PATH = ROOT / "configs" / "research_evidence_policy.json"
STICKY_MEDIUM_HORIZON_ROUNDS = 3


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



def _promotion_quality_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = ((snapshot.get('promotion_scorecard') or {}).get('rows') or [])
    return {row.get('factor_name'): row for row in rows if row.get('factor_name')}



def _approved_universe_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = ((snapshot.get('approved_universe') or {}).get('rows') or [])
    return {row.get('factor_name'): row for row in rows if row.get('factor_name')}



def _approved_universe_names(snapshot: dict[str, Any]) -> list[str]:
    return [name for name in (snapshot.get('approved_universe_names') or []) if name]


def _novelty_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = ((snapshot.get('novelty_judge') or {}).get('rows') or [])
    return {row.get('candidate_name'): row for row in rows if row.get('candidate_name')}


def _failure_dossier_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get('candidate_failure_dossiers', []) or []
    direct_map = {row.get('candidate_name'): row for row in rows if row.get('candidate_name')}
    aggregated_map = dict(snapshot.get('representative_failure_dossiers') or {})
    merged = dict(direct_map)
    for name, row in aggregated_map.items():
        if not name:
            continue
        merged[name] = {**direct_map.get(name, {}), **(row or {})}
    return merged



def _failure_enhancement_maps(snapshot: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    payload = snapshot.get('failure_analyst_enhancement') or {}
    stop_map = {
        row.get('candidate_name'): row
        for row in (payload.get('stop_or_continue_recommendation') or [])
        if row.get('candidate_name')
    }
    reroute_map = {
        row.get('candidate_name'): row
        for row in (payload.get('reroute_proposals') or [])
        if row.get('candidate_name')
    }
    question_cards_v2 = list(payload.get('question_cards_v2') or [])
    return stop_map, reroute_map, question_cards_v2



def _attach_quality_context(row: dict[str, Any], quality_map: dict[str, dict[str, Any]], failure_map: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    candidate_name = row.get('candidate_name')
    quality = quality_map.get(candidate_name) or {}
    failure = (failure_map or {}).get(candidate_name) or {}
    if not quality and not failure:
        return row
    merged = dict(row)
    merged['quality_total_score'] = quality.get('quality_total_score')
    merged['quality_classification'] = quality.get('quality_classification')
    merged['quality_classification_label'] = quality.get('quality_classification_label')
    merged['quality_promotion_decision'] = quality.get('quality_promotion_decision')
    merged['quality_summary'] = quality.get('quality_summary')
    merged['quality_scores'] = quality.get('quality_scores') or {}
    merged['raw_rank_ic_mean'] = quality.get('raw_rank_ic_mean')
    merged['neutralized_rank_ic_mean'] = quality.get('neutralized_rank_ic_mean')
    merged['retention_industry'] = quality.get('retention_industry')
    merged['candidate_status'] = quality.get('candidate_status') or merged.get('candidate_status')
    merged['approved_universe_member'] = quality.get('approved_universe_member')
    merged['approved_universe_reason'] = quality.get('approved_universe_reason')
    if failure:
        merged['failure_dossier'] = failure
        merged['failure_modes'] = failure.get('failure_modes') or []
        merged['failure_recommended_action'] = failure.get('recommended_action')
        merged['regime_dependency'] = failure.get('regime_dependency')
        merged['parent_delta_status'] = failure.get('parent_delta_status')
    return merged



def _watchlist_focus_candidates(snapshot: dict[str, Any], frontier_suppressed: set[str], stable_candidates: list[str], failure_map: dict[str, dict[str, Any]]) -> list[str]:
    rows = ((snapshot.get('promotion_scorecard') or {}).get('rows') or [])
    stable_set = set(stable_candidates)
    ranked: list[tuple[float, str]] = []
    for row in rows:
        name = row.get('factor_name')
        if not name or name in frontier_suppressed or name in stable_set:
            continue
        decision = row.get('decision_key')
        quality_classification = row.get('quality_classification')
        failure = failure_map.get(name) or {}
        if failure.get('recommended_action') == 'suppress':
            continue
        if decision not in {'validate_now', 'watchlist', 'regime_sensitive'} and quality_classification not in {'needs-validation', 'regime-sensitive', 'validate-only'}:
            continue
        recent_score = float(row.get('latest_recent_final_score') or row.get('latest_final_score') or 0.0)
        promotion_score = float(row.get('promotion_score') or 0.0)
        ranked.append((recent_score + promotion_score / 100.0, name))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    ordered: list[str] = []
    for _, name in ranked:
        if name not in ordered:
            ordered.append(name)
    return ordered[:3]



def _borderline_focus_candidates(
    snapshot: dict[str, Any],
    frontier_suppressed: set[str],
    stable_candidates: list[str],
    failure_map: dict[str, dict[str, Any]],
    novelty_map: dict[str, dict[str, Any]],
    approved_universe_names: list[str],
) -> list[str]:
    report_path = ROOT / 'artifacts' / 'quality_not_proven_root_cause_report.json'
    if report_path.exists():
        report = read_json(report_path)
        report_names = [row.get('candidate_name') for row in (report.get('top_borderline_candidates') or []) if row.get('candidate_name')]
        filtered = [name for name in report_names if name not in frontier_suppressed and name not in set(stable_candidates) and name not in set(approved_universe_names)]
        if filtered:
            return filtered[:3]
    rows = ((snapshot.get('promotion_scorecard') or {}).get('rows') or [])
    stable_set = set(stable_candidates)
    approved_set = set(approved_universe_names)
    ranked: list[tuple[float, str]] = []
    for row in rows:
        name = row.get('factor_name')
        if not name or name in frontier_suppressed or name in stable_set or name in approved_set:
            continue
        novelty = novelty_map.get(name) or {}
        if novelty.get('novelty_class') not in {'near_neighbor_soft', 'meaningful_extension', 'meaningful_extension_low_confidence'}:
            continue
        failure = failure_map.get(name) or {}
        hard_flags = row.get('quality_hard_flags') or {}
        if failure.get('recommended_action') == 'suppress' and novelty.get('recommended_action') == 'suppress':
            continue
        if hard_flags.get('non_incremental_vs_parent') and novelty.get('novelty_class') != 'meaningful_extension_low_confidence':
            continue
        recent_score = float(row.get('latest_recent_final_score') or row.get('latest_final_score') or 0.0)
        total_score = float(row.get('quality_total_score') or 0.0)
        incrementality = float((row.get('quality_scores') or {}).get('incremental_value') or 0.0)
        ranked.append((total_score + recent_score + incrementality, name))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [name for _, name in ranked[:3]]


def _load_sticky_medium_horizon(strategy_memory: dict[str, Any], frontier_suppressed: set[str]) -> list[str]:
    sticky = []
    for row in (strategy_memory.get('sticky_medium_horizon_candidates') or []):
        name = row.get('candidate_name')
        rounds_remaining = int(row.get('rounds_remaining') or 0)
        if not name or rounds_remaining <= 0 or name in frontier_suppressed:
            continue
        sticky.append(name)
    return sticky



def _build_sticky_medium_horizon_payload(active_names: list[str], existing_payload: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    existing_by_name = {row.get('candidate_name'): row for row in (existing_payload or []) if row.get('candidate_name')}
    payload: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name in active_names:
        if not name or name in seen:
            continue
        seen.add(name)
        previous = existing_by_name.get(name) or {}
        previous_rounds = int(previous.get('rounds_remaining') or 0)
        payload.append({
            'candidate_name': name,
            'rounds_remaining': max(previous_rounds - 1, 0) if previous_rounds else STICKY_MEDIUM_HORIZON_ROUNDS,
        })
    return payload


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


def _candidate_evidence_gate(candidate_context: dict[str, Any], evidence_policy: dict[str, Any]) -> dict[str, Any]:
    gate = dict(candidate_context.get('acceptance_gate') or {})
    frontier_gate = evidence_policy.get('frontier_gate') or {}
    status = gate.get('status') or frontier_gate.get('missing_status') or 'missing'
    pass_statuses = set(frontier_gate.get('pass_statuses') or ['pass'])
    validation_statuses = set(frontier_gate.get('validation_statuses') or ['monitor', 'blocked'])
    if status in pass_statuses:
        action = 'frontier_ok'
    elif status in validation_statuses:
        action = 'needs_validation'
    else:
        action = 'evidence_missing'
    return {
        'status': status,
        'action': action,
        'explanation': gate.get('explanation') or gate.get('promotion') or 'acceptance gate missing or incomplete',
    }


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



def _representative_scope_key(task: dict[str, Any]) -> str | None:
    payload = task.get('payload') or {}
    focus_names = sorted(
        {
            name
            for name in (payload.get('focus_factors') or [])
            if name
        }
        | {
            row.get('candidate_name')
            for row in (task.get('focus_candidates') or [])
            if row.get('candidate_name')
        }
    )
    if not focus_names:
        return None
    category = str(task.get('category') or 'validation')
    diagnostic_type = str(payload.get('diagnostic_type') or '')
    if diagnostic_type == 'approved_universe_candidate_review':
        return f"{category}::approved_universe::{'|'.join(focus_names)}"
    return f"{category}::{'|'.join(focus_names)}"


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



def _representative_focus_candidates(names: list[str], candidate_context_by_name: dict[str, dict[str, Any]], cluster_rep_map: dict[str, dict[str, Any]], failure_map: dict[str, dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    representative_names, representative_notes = _prefer_representatives(names, candidate_context_by_name, cluster_rep_map)
    cleaned = []
    seen: set[str] = set()
    for name in representative_names:
        if not name or name in seen:
            continue
        seen.add(name)
        failure = failure_map.get(name) or {}
        if failure.get('recommended_action') == 'suppress':
            continue
        cleaned.append(name)
    return cleaned, representative_notes


def _quality_priority_context(
    stable_candidates: list[str],
    frontier_preferred: list[str],
    frontier_robust: list[str],
    frontier_soft_robust: list[str],
    candidate_context_by_name: dict[str, dict[str, Any]],
    evidence_policy: dict[str, Any],
    relationship_summary: dict[str, Any],
    research_learning: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    focus_names: list[str] = []
    for group in (stable_candidates, frontier_robust, frontier_soft_robust, frontier_preferred):
        for name in group:
            if name and name not in focus_names:
                focus_names.append(name)
    focus_names = focus_names[:4]

    evidence_missing: list[str] = []
    needs_validation: list[str] = []
    for name in focus_names:
        gate = _candidate_evidence_gate(candidate_context_by_name.get(name, {}), evidence_policy)
        if gate['action'] == 'evidence_missing':
            evidence_missing.append(name)
        elif gate['action'] == 'needs_validation':
            needs_validation.append(name)

    duplicate_pressure = int(relationship_summary.get('duplicate_of', 0) or 0)
    refinement_pressure = int(relationship_summary.get('refinement_of', 0) or 0)
    high_corr_pressure = int(relationship_summary.get('high_corr', 0) or 0)
    research_mode = ((research_learning.get('research_mode') or {}).get('mode') or 'balanced')
    floor = exploration_floor_context(snapshot)
    regime_context = build_regime_context(snapshot)
    regime = str(regime_context.get('regime') or 'neutral')

    quality_priority_mode = bool(evidence_missing or duplicate_pressure >= 8 or research_mode == 'diagnosis_heavy')
    freeze_exploration = bool(floor['true_fault_recovery'])
    generated_candidate_budget = int(floor['exploration_floor_slots'] or 0)
    if not freeze_exploration:
        if quality_priority_mode:
            generated_candidate_budget = max(generated_candidate_budget, 1)
            if regime in {'crowded_frontier', 'regime_sensitive_frontier'}:
                generated_candidate_budget = max(generated_candidate_budget, 2)
        elif regime == 'expansion_ready':
            generated_candidate_budget = max(generated_candidate_budget, 3)

    reasons: list[str] = []
    if evidence_missing:
        reasons.append(f"frontier evidence missing: {', '.join(evidence_missing)}")
    if needs_validation:
        reasons.append(f"frontier needs validation: {', '.join(needs_validation)}")
    if duplicate_pressure >= 8:
        reasons.append(f'duplicate pressure={duplicate_pressure}')
    if refinement_pressure >= 250:
        reasons.append(f'refinement pressure={refinement_pressure}')
    if research_mode == 'diagnosis_heavy':
        reasons.append('research_mode=diagnosis_heavy')
    if regime != 'neutral':
        reasons.append(f'regime={regime}')

    generated_candidate_budget_by_pool = split_exploration_pool_budget(
        generated_candidate_budget,
        prioritize_new_mechanism=bool((research_learning.get('failure_question_cards') or []) or (research_learning.get('representative_failure_dossiers') or {})),
        quality_priority_mode=quality_priority_mode,
        regime=regime,
    )

    return {
        'quality_priority_mode': quality_priority_mode,
        'freeze_exploration': freeze_exploration,
        'generated_candidate_budget': generated_candidate_budget,
        'generated_candidate_budget_by_pool': generated_candidate_budget_by_pool,
        'frontier_focus_names': focus_names,
        'frontier_evidence_missing': evidence_missing,
        'frontier_needs_validation': needs_validation,
        'duplicate_pressure': duplicate_pressure,
        'refinement_pressure': refinement_pressure,
        'high_corr_pressure': high_corr_pressure,
        'research_mode': research_mode,
        'reasons': reasons,
        'exploration_floor': floor,
        'regime_context': regime_context,
    }


def build_research_candidate_pool(snapshot_path: str | Path, output_path: str | Path, branch_plan_path: str | Path | None = None) -> dict[str, Any]:
    snapshot = read_json(snapshot_path)
    registry_path = ROOT / 'artifacts' / 'research_space_registry.json'
    space_map_path = ROOT / 'artifacts' / 'research_space_map.json'
    branch_plan = read_json(branch_plan_path) if branch_plan_path and Path(branch_plan_path).exists() else {}
    space_map = read_json(space_map_path) if space_map_path.exists() else {}
    strategy_memory_path = ROOT / 'artifacts' / 'research_memory.json'
    strategy_memory = read_json(strategy_memory_path) if strategy_memory_path.exists() else {}
    archived_branches = set(strategy_memory.get('archived_branches', []) or [])
    agent_control = strategy_memory.get('agent_control') or {}
    agent_should_stop = set(agent_control.get('should_stop') or [])
    agent_suppress_families = set(agent_control.get('suppress_families') or [])
    agent_should_reroute = list(agent_control.get('should_reroute') or [])
    agent_hypothesis_cards = list(agent_control.get('hypothesis_cards') or [])
    challenger_queue = set(agent_control.get('challenger_queue') or [])
    reroute_targets = set()
    reroute_sources = set()
    for row in agent_should_reroute:
        if isinstance(row, str) and '->' in row:
            source, target = row.split('->', 1)
            reroute_sources.add(source.strip())
            reroute_targets.add(target.strip())

    existing_fingerprints = {task.get('fingerprint') for task in snapshot.get('recent_research_tasks', [])}
    existing_signatures = _existing_signatures(snapshot)
    latest_run = snapshot.get('latest_run') or {}
    generated_configs = set(snapshot.get('generated_configs', []))
    frontier_focus = snapshot.get('frontier_focus') or {}
    frontier_preferred = [name for name in (frontier_focus.get('short_window_candidates') or frontier_focus.get('preferred_candidates') or []) if name]
    frontier_robust = [name for name in (frontier_focus.get('robust_candidates') or []) if name]
    frontier_soft_robust = [name for name in (frontier_focus.get('soft_robust_candidates') or []) if name]
    frontier_secondary = [name for name in (frontier_focus.get('secondary_candidates') or []) if name]
    frontier_suppressed = set(frontier_focus.get('suppressed_candidates') or [])
    approved_universe_map = _approved_universe_map(snapshot)
    approved_universe_names = _approved_universe_names(snapshot)
    novelty_map = _novelty_map(snapshot)
    failure_stop_map, failure_reroute_map, failure_question_cards_v2 = _failure_enhancement_maps(snapshot)
    sticky_medium_horizon = _load_sticky_medium_horizon(strategy_memory, frontier_suppressed)
    raw_stable_candidates = approved_universe_names[:5] or frontier_robust[:5] or frontier_soft_robust[:5] or frontier_preferred[:5] or [row['factor_name'] for row in snapshot.get('stable_candidates', [])[:5]]
    latest_graveyard = [name for name in (snapshot.get('latest_graveyard', []) or []) if name not in frontier_suppressed][:5]
    queue_budget = snapshot.get('queue_budget', {})
    exploration_state = snapshot.get('exploration_state', {})
    failure_state = snapshot.get('failure_state', {})
    candidate_context_by_name = _candidate_context_by_name(snapshot)
    family_score_map = _family_score_map(snapshot)
    cluster_rep_map = _cluster_rep_map(snapshot)
    quality_map = _promotion_quality_map(snapshot)
    failure_map = _failure_dossier_map(snapshot)
    relationship_summary = snapshot.get('relationship_summary', {}) or {}
    family_recommendations = {row.get('family'): row for row in snapshot.get('family_recommendations', []) if row.get('family')}
    trial_summary = snapshot.get('research_trial_summary', {}) or {}
    analyst_signals = snapshot.get('analyst_signals') or {}
    research_learning = build_research_learning(strategy_memory_path)
    autonomy_policy = read_json(AUTONOMY_POLICY_PATH) if AUTONOMY_POLICY_PATH.exists() else {}
    evidence_policy = read_json(EVIDENCE_POLICY_PATH) if EVIDENCE_POLICY_PATH.exists() else {}
    candidate_generation_plan = build_candidate_generation_plan(snapshot_path, strategy_memory_path, ROOT / "artifacts" / "candidate_generation_plan.json")
    generated_candidate_tasks = compile_candidate_generation_plan(ROOT / "artifacts" / "candidate_generation_plan.json")
    learning_families = research_learning.get('families') or {}
    representative_candidate_stats = research_learning.get('representative_candidate_stats') or {}
    promotion = should_promote_research_paths()
    promotable_families = set(promotion.get('promotable_families') or [])

    stable_candidates, representative_notes = _representative_focus_candidates(raw_stable_candidates, candidate_context_by_name, cluster_rep_map, failure_map)
    stable_candidates = [name for name in stable_candidates if name in approved_universe_names or name not in frontier_suppressed]
    if not stable_candidates and frontier_secondary:
        stable_candidates, extra_notes = _representative_focus_candidates(frontier_secondary[:5], candidate_context_by_name, cluster_rep_map, failure_map)
        stable_candidates = [name for name in stable_candidates if name not in frontier_suppressed]
        representative_notes.extend(extra_notes)
    quality_priority = _quality_priority_context(
        stable_candidates,
        frontier_preferred,
        frontier_robust,
        frontier_soft_robust,
        candidate_context_by_name,
        evidence_policy,
        relationship_summary,
        research_learning,
        snapshot,
    )
    analyst_focus = set(analyst_signals.get('focus_factors') or [])
    analyst_core = set(analyst_signals.get('keep_as_core_candidates') or [])
    analyst_graveyard = set(analyst_signals.get('review_graveyard') or [])
    must_validate_before_expand = bool(analyst_signals.get('must_validate_before_expand'))

    base_config = read_json(ROOT / 'configs' / 'tushare_workflow.json')
    end_date = latest_run.get('end_date') or base_config['end_date']
    family_progress = space_map.get('family_progress', {})
    selected_families = set(branch_plan.get('selected_families', []))

    candidates: list[dict[str, Any]] = []
    suppressed_tasks: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    seen_representative_scopes: set[str] = set()

    def append_task(task: dict[str, Any], suppression_reason: str | None = None) -> None:
        signature = _dedupe_signature(task)
        branch_id = task.get('branch_id') or (task.get('payload') or {}).get('branch_id')
        family_key = _task_family_key(task)
        learning = learning_families.get(family_key) or {}
        if learning.get('cooldown_active'):
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': 'family_learning_cooldown',
                'branch_id': branch_id,
                'family_key': family_key,
                'learning': learning,
            })
            return
        if family_key in agent_suppress_families or family_key in reroute_sources:
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': 'agent_suppressed_family' if family_key in agent_suppress_families else 'agent_rerouted_family',
                'branch_id': branch_id,
                'family_key': family_key,
            })
            return
        if branch_id and branch_id in agent_should_stop:
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': 'agent_should_stop_branch',
                'branch_id': branch_id,
                'family_key': family_key,
            })
            return
        if family_key in reroute_targets:
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - 8)
            task['reason'] += f" agent reroute: 当前优先把算力转向 {family_key}。"
        focus_names = set((task.get('payload') or {}).get('focus_factors') or [])
        focus_names.update({row.get('candidate_name') for row in (task.get('focus_candidates') or []) if row.get('candidate_name')})
        card_matches = [row for row in agent_hypothesis_cards if row.get('candidate_name') in focus_names or (row.get('family') and row.get('family') == family_key)]
        if card_matches:
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - min(6, len(card_matches) * 2))
            task['reason'] += f" agent hypothesis card 命中 {', '.join([row.get('candidate_name') for row in card_matches if row.get('candidate_name')][:3])}。"
        enhanced_cards = [row for row in failure_question_cards_v2 if row.get('candidate_name') in focus_names]
        if enhanced_cards:
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - min(6, len(enhanced_cards) * 2))
            task['reason'] += f" failure_analyst question card v2 命中 {', '.join([row.get('candidate_name') for row in enhanced_cards if row.get('candidate_name')][:3])}。"
        if focus_names & challenger_queue:
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - 4)
            task['reason'] += f" challenger_queue 命中 {', '.join(sorted(focus_names & challenger_queue))}。"
        stop_hits = [name for name in focus_names if (failure_stop_map.get(name) or {}).get('recommendation') == 'stop']
        reroute_hits = [name for name in focus_names if failure_reroute_map.get(name)]
        if stop_hits and task.get('category') in {'exploration', 'baseline'}:
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': 'failure_analyst_stop_recommendation',
                'branch_id': branch_id,
                'focus_candidates': sorted(stop_hits),
            })
            return
        if reroute_hits:
            if task.get('category') == 'validation':
                task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - 5)
            elif task.get('category') == 'exploration':
                task['priority_hint'] = int(task.get('priority_hint', 50)) + 4
            task['reason'] += f" failure_analyst reroute 命中 {', '.join(sorted(reroute_hits)[:3])}。"
        approved_focus = sorted(focus_names & set(approved_universe_names))
        novelty_focus = [novelty_map.get(name) or {} for name in sorted(focus_names) if novelty_map.get(name)]
        if novelty_focus:
            suppress_like = [row.get('candidate_name') for row in novelty_focus if row.get('recommended_action') == 'suppress' and row.get('candidate_name')]
            promote_like = [row.get('candidate_name') for row in novelty_focus if row.get('recommended_action') == 'promote' and row.get('candidate_name')]
            if suppress_like and task.get('category') == 'exploration':
                task['priority_hint'] = int(task.get('priority_hint', 50)) + min(10, len(suppress_like) * 4)
                task['reason'] += f" novelty_judge: suppress {', '.join(suppress_like[:3])}。"
            elif promote_like and task.get('category') == 'validation':
                task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - min(8, len(promote_like) * 3))
                task['reason'] += f" novelty_judge: promote {', '.join(promote_like[:3])}。"
            task['novelty_alignment'] = {
                'focus': [row.get('candidate_name') for row in novelty_focus if row.get('candidate_name')],
                'class_summary': sorted({row.get('novelty_class') for row in novelty_focus if row.get('novelty_class')}),
                'recommended_actions': sorted({row.get('recommended_action') for row in novelty_focus if row.get('recommended_action')}),
            }
        if approved_focus:
            approved_rows = [approved_universe_map.get(name) or {} for name in approved_focus]
            approved_states = sorted({(row.get('lifecycle_state') or row.get('universe_state') or 'approved') for row in approved_rows})
            governance_actions = sorted({row.get('governance_action') for row in approved_rows if row.get('governance_action')})
            budget_weight = round(sum(float((row.get('allocated_weight') or row.get('portfolio_weight_hint') or 0.0)) for row in approved_rows), 6)
            hard_reject_overlap = [name for name, row in zip(approved_focus, approved_rows) if (row.get('lifecycle_state') or row.get('universe_state')) == 'rejected']
            if hard_reject_overlap and task.get('category') in {'exploration', 'baseline'}:
                suppressed_tasks.append({
                    'fingerprint': task.get('fingerprint'),
                    'signature': signature,
                    'worker_note': task.get('worker_note'),
                    'reason': 'approved_universe_rejected_state',
                    'branch_id': branch_id,
                    'approved_focus': hard_reject_overlap,
                })
                return
            state_bonus = 8 if 'approved' in approved_states and task.get('category') == 'validation' else 4 if 'watchlist' in approved_states and task.get('category') == 'validation' else -4 if 'shadow' in approved_states and task.get('category') == 'exploration' else 2
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - state_bonus)
            task['reason'] += f" approved universe 命中 {', '.join(approved_focus)}，state={','.join(approved_states)}，budget≈{budget_weight}。"
            task.setdefault('approved_universe_alignment', {}).update({
                'focus_overlap': approved_focus,
                'state_summary': approved_states,
                'governance_actions': governance_actions,
                'budget_weight': budget_weight,
            })
        if learning.get('recommended_action') == 'upweight':
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - 4)
            task['reason'] += f" research learning: {family_key} 最近有效，优先级上调。"
        if family_key in promotable_families:
            task['priority_hint'] = max(1, int(task.get('priority_hint', 50)) - 3)
            task['reason'] += f" research promoter: recovery 后优先扩展 {family_key} 方向。"
        elif learning.get('recommended_action') == 'downweight':
            task['priority_hint'] = int(task.get('priority_hint', 50)) + 5
            task['reason'] += f" research learning: {family_key} 最近无增益偏多，优先级下调。"
        task_source = (task.get('payload') or {}).get('source')
        if branch_id and branch_id in archived_branches and task_source != 'sticky_medium_horizon':
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': 'archived_branch_suppressed',
                'branch_id': branch_id,
            })
            return
        if task.get('fingerprint') in existing_fingerprints or signature in existing_signatures or signature in seen_signatures:
            expected_gain = set(task.get('expected_knowledge_gain') or (task.get('payload') or {}).get('expected_information_gain') or [])
            objectives = set(((autonomy_policy.get('principles') or {}).get('objective') or []))
            high_info_repeat = ('epistemic_gain' in objectives) and bool(expected_gain & {'search_space_reduced', 'boundary_confirmed', 'new_branch_opened', 'repeated_graveyard_confirmed', 'stable_candidate_confirmed'})
            if not high_info_repeat:
                suppressed_tasks.append({
                    'fingerprint': task.get('fingerprint'),
                    'signature': signature,
                    'worker_note': task.get('worker_note'),
                    'reason': suppression_reason or 'duplicate_candidate_suppressed',
                    'branch_id': branch_id,
                })
                return
        representative_scope = _representative_scope_key(task)
        if representative_scope and representative_scope in seen_representative_scopes:
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': 'representative_scope_duplicate_suppressed',
                'branch_id': branch_id,
                'representative_scope': representative_scope,
            })
            return
        seen_signatures.add(signature)
        if representative_scope:
            seen_representative_scopes.add(representative_scope)
            task['representative_scope'] = representative_scope
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
            if must_validate_before_expand:
                task['priority_hint'] += 10
            task['reason'] += f" 当前候选图中有 {relationship_summary.get('hybrid_of', 0)} 条 hybrid 关系、{len(snapshot.get('candidate_clusters', []) or [])} 个 cluster；但高风险 family={validate_risk_family_count} 个，因此扩窗优先级被下调，先确认结构是否真的跨阶段成立。" + (" analyst 要求先验证再扩窗。" if must_validate_before_expand else "")
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
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
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
            append_task(task, 'recent_window_already_covered')

    medium_focus_candidates = []
    medium_focus_from_sticky = []
    for name in frontier_soft_robust:
        if name and name not in frontier_suppressed and name not in medium_focus_candidates:
            medium_focus_candidates.append(name)
    for name in sticky_medium_horizon:
        if name and name not in frontier_suppressed and name not in medium_focus_candidates:
            medium_focus_candidates.append(name)
            medium_focus_from_sticky.append(name)
    medium_focus_candidates, medium_rep_notes = _representative_focus_candidates(medium_focus_candidates, candidate_context_by_name, cluster_rep_map, failure_map)
    representative_notes.extend(medium_rep_notes)
    medium_focus_from_sticky = [name for name in medium_focus_from_sticky if name in medium_focus_candidates]
    if medium_focus_candidates:
        medium_level = 1
        medium_tasks = build_medium_horizon_task(medium_level, medium_focus_candidates, latest_run, end_date, base_config, existing_fingerprints, generated_configs)
        for task in medium_tasks:
            focus_context = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in medium_focus_candidates]
            task['priority_hint'] -= 2
            for row in focus_context:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
            task['focus_candidates'] = focus_context
            task['relationship_signal'] = {
                'soft_robust_count': len(medium_focus_candidates),
                'sticky_medium_horizon_count': len(medium_focus_from_sticky),
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'refinement_count': int(relationship_summary.get('refinement_of', 0)),
            }
            task['payload']['source'] = 'sticky_medium_horizon' if medium_focus_from_sticky else 'soft_robust_frontier'
            task['reason'] += (
                f" 当前 soft robust/sticky 候选={', '.join(medium_focus_candidates)}，需要单独确认 60d/90d/120d 是否还能活下来，"
                f"而不是继续混在短窗前沿里。 refinement={relationship_summary.get('refinement_of', 0)}，duplicate={relationship_summary.get('duplicate_of', 0)}。"
                + (f" sticky 候选命中={', '.join(medium_focus_from_sticky)}，即使本轮 frontier 波动也保留中窗晋级赛。" if medium_focus_from_sticky else '')
            )
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
            append_task(task, 'medium_horizon_validation_already_covered')

    watchlist_candidates = approved_universe_names[:3] + [name for name in _watchlist_focus_candidates(snapshot, frontier_suppressed, stable_candidates, failure_map) if name not in set(approved_universe_names[:3])]
    watchlist_candidates, watchlist_rep_notes = _representative_focus_candidates(watchlist_candidates, candidate_context_by_name, cluster_rep_map, failure_map)
    representative_notes.extend(watchlist_rep_notes)

    fragile_priority_names: list[str] = []
    for group in (stable_candidates, watchlist_candidates, frontier_soft_robust, frontier_preferred):
        for name in group:
            if name and name not in fragile_priority_names and name not in frontier_suppressed:
                ctx = candidate_context_by_name.get(name, {})
                if ctx.get('fragile'):
                    fragile_priority_names.append(name)
    fragile_priority_names.sort(
        key=lambda name: (
            -float((_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map).get('quality_total_score') or 0.0)),
            -float((_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map).get('latest_recent_final_score') or 0.0)),
            name,
        )
    )
    fragile_priority_names, fragile_rep_notes = _representative_focus_candidates(fragile_priority_names[:3], candidate_context_by_name, cluster_rep_map, failure_map)
    representative_notes.extend(fragile_rep_notes)
    if fragile_priority_names:
        fragile_tasks = build_fragile_candidate_task(1, fragile_priority_names[:2], existing_fingerprints)
        for task in fragile_tasks:
            focus_context = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in fragile_priority_names[:2]]
            for row in focus_context:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
            task['priority_hint'] -= 8
            if analyst_core & set(fragile_priority_names):
                task['priority_hint'] -= 4
            task['focus_candidates'] = focus_context
            task['relationship_signal'] = {
                'fragile_candidate_count': len(fragile_priority_names[:2]),
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'refinement_count': int(relationship_summary.get('refinement_of', 0)),
            }
            task['analyst_alignment'] = {
                'focus_overlap': sorted(analyst_focus & set(fragile_priority_names)),
                'core_overlap': sorted(analyst_core & set(fragile_priority_names)),
                'graveyard_overlap': sorted(analyst_graveyard & set(fragile_priority_names)),
            }
            task['reason'] += (
                f" 当前 fragile 候选={', '.join(fragile_priority_names[:2])}，需要专项加固验证，先确认谁能穿过中窗/风险约束，"
                f"再决定是否继续扩展。 duplicate={relationship_summary.get('duplicate_of', 0)}，refinement={relationship_summary.get('refinement_of', 0)}。"
                + (f" analyst 核心候选命中 {', '.join(sorted(analyst_core & set(fragile_priority_names)))}。" if analyst_core & set(fragile_priority_names) else '')
            )
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
            append_task(task, 'fragile_candidate_hardening_already_covered')

    if watchlist_candidates and ('watchlist_candidate_validation' in selected_families or not selected_families):
        watchlist_tasks = build_watchlist_candidate_task(1, watchlist_candidates, latest_run, end_date, base_config, existing_fingerprints, generated_configs)
        for task in watchlist_tasks:
            focus_context = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in watchlist_candidates]
            for row in focus_context:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
            max_incremental = max([float((row.get('quality_scores') or {}).get('incremental_value') or 0.0) for row in focus_context] or [0.0])
            max_recent_score = max([float(row.get('quality_total_score') or 0.0) for row in focus_context] or [0.0])
            task['priority_hint'] += _priority_adjustment(max_recent_score, 0, 0)
            if analyst_focus & set(watchlist_candidates):
                task['priority_hint'] -= 4
            task['focus_candidates'] = focus_context
            task['relationship_signal'] = {
                'watchlist_candidate_count': len(watchlist_candidates),
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'refinement_count': int(relationship_summary.get('refinement_of', 0)),
                'max_incremental_value': max_incremental,
            }
            task['analyst_alignment'] = {
                'focus_overlap': sorted(analyst_focus & set(watchlist_candidates)),
                'core_overlap': sorted(analyst_core & set(watchlist_candidates)),
                'graveyard_overlap': sorted(analyst_graveyard & set(watchlist_candidates)),
            }
            task['reason'] += (
                f" 当前 watchlist 候选={', '.join(watchlist_candidates)}，最高 incremental_value={max_incremental:.1f}，"
                f"需要递进窗口晋级，避免长期停在 observation 状态。 duplicate={relationship_summary.get('duplicate_of', 0)}，refinement={relationship_summary.get('refinement_of', 0)}。"
                + (f" analyst focus 命中 {', '.join(sorted(analyst_focus & set(watchlist_candidates)))}。" if analyst_focus & set(watchlist_candidates) else '')
            )
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
            append_task(task, 'watchlist_validation_already_covered')

    borderline_candidates = _borderline_focus_candidates(snapshot, frontier_suppressed, stable_candidates, failure_map, novelty_map, approved_universe_names)
    if borderline_candidates:
        task = make_task(
            task_type='diagnostic',
            category='validation',
            priority_hint=14,
            reason='针对最接近晋级但仍未被证明的候选做定向验证，避免继续混在泛探索里。',
            expected_knowledge_gain=['borderline_candidate_validated', 'boundary_confirmed'],
            payload={
                'diagnostic_type': 'borderline_candidate_targeted_validation',
                'focus_factors': borderline_candidates,
                'reasons': ['quality_not_proven_root_cause', 'targeted_validation'],
                'source_output_dir': latest_run.get('output_dir') or 'artifacts/tushare_batch',
            },
            worker_note='validation｜borderline 候选定向验证',
            goal='targeted_validate_borderline_candidates',
            hypothesis='最接近晋级但仍未被证明的候选，需要专项补中长窗、neutralized 与 persistence 证据。',
            branch_id='borderline_candidate_targeted_validation',
            stop_if=['borderline_candidates_fail_targeted_validation_twice'],
            promote_if=['borderline_candidate_boundary_confirmed'],
            disconfirm_if=['borderline_candidates_remain_unproven_after_targeted_validation'],
        )
        focus_context = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in borderline_candidates]
        for row in focus_context:
            if row:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
                if novelty_map.get(row.get('candidate_name')):
                    row['novelty_judgment'] = novelty_map.get(row.get('candidate_name'))
        task['focus_candidates'] = focus_context
        task['relationship_signal'] = {
            'borderline_candidate_count': len(borderline_candidates),
            'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
            'refinement_count': int(relationship_summary.get('refinement_of', 0)),
        }
        task['reason'] += f" 当前 borderline 候选={', '.join(borderline_candidates)}，优先补最接近晋级的证据，而不是扩大泛探索。"
        append_task(task, 'borderline_validation_already_covered')

    approved_focus_candidates = approved_universe_names[:3]
    if approved_focus_candidates:
        task = make_task(
            task_type='diagnostic',
            category='validation',
            priority_hint=18,
            reason='Approved Universe 候选需要持续维护验证，避免入池后长期失真或失效。',
            expected_knowledge_gain=['approved_universe_candidate_maintained', 'approved_universe_boundary_confirmed'],
            payload={
                'diagnostic_type': 'approved_universe_candidate_review',
                'focus_factors': approved_focus_candidates,
                'reasons': ['approved_universe_followup'],
                'source_output_dir': latest_run.get('output_dir') or 'artifacts/tushare_batch',
            },
            worker_note='validation｜Approved Universe 候选维护验证',
            goal='maintain_approved_universe_candidates',
            hypothesis='已进入 approved universe 的候选应被持续补证据、复核边界，并在失效前被尽早发现。',
            branch_id='approved_universe_candidate_maintenance',
            stop_if=['approved_universe_candidate_repeatedly_fails_validation'],
            promote_if=['approved_universe_boundary_confirmed'],
            disconfirm_if=['approved_universe_candidate_loses_cross_window_support'],
        )
        focus_context = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in approved_focus_candidates]
        for row in focus_context:
            if row:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
        task['focus_candidates'] = focus_context
        task['approved_universe_alignment'] = {
            'focus_overlap': approved_focus_candidates,
            'selection_policy_version': (snapshot.get('approved_universe') or {}).get('selection_policy_version'),
        }
        task['relationship_signal'] = {
            'approved_candidate_count': len(approved_focus_candidates),
            'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
            'refinement_count': int(relationship_summary.get('refinement_of', 0)),
        }
        task['reason'] += f" 当前 approved universe 命中 {', '.join(approved_focus_candidates)}，这些候选应优先补边界验证，而不是退回到普通 watchlist 节奏。"
        task['hypothesis'] = task['payload'].get('hypothesis')
        task['goal'] = task['payload'].get('goal')
        task['branch_id'] = task['payload'].get('branch_id')
        append_task(task, 'approved_universe_followup_already_covered')

    if stable_level and ('stable_candidate_validation' in selected_families or not selected_families):
        stable_tasks = build_stable_candidate_task(stable_level, stable_candidates, existing_fingerprints)
        for task in stable_tasks:
            focus_context = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in stable_candidates]
            for row in focus_context:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
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
            if analyst_core & set(stable_candidates):
                task['priority_hint'] -= 6
            if analyst_focus & set(stable_candidates):
                task['priority_hint'] -= 3
            task['analyst_alignment'] = {
                'focus_overlap': sorted(analyst_focus & set(stable_candidates)),
                'core_overlap': sorted(analyst_core & set(stable_candidates)),
                'graveyard_overlap': sorted(analyst_graveyard & set(stable_candidates)),
            }
            task['reason'] += (
                f" 重点候选累计关系 {relationship_count} 条、lineage {lineage_count} 条"
                + (f"，最强 family={strongest_family}" if strongest_family else "")
                + (f"，trial_pressure={strongest_trial.get('trial_pressure')}，false_positive_pressure={strongest_trial.get('false_positive_pressure')}" if strongest_trial else "")
                + (f"，fragile_candidates={fragile_count}，family_risk_score={family_risk_score}" if fragile_count or family_risk_score is not None else "")
                + (f"。frontier 短窗优先 family={', '.join((frontier_focus.get('short_window_families') or frontier_focus.get('preferred_families') or [])[:3])}，短窗候选={', '.join(frontier_preferred[:4])}。" if frontier_preferred else "")
                + (f" 稳健前沿候选={', '.join(frontier_robust[:4])}。" if frontier_robust else "")
                + (f" 软稳健前沿候选={', '.join(frontier_soft_robust[:4])}。" if frontier_soft_robust else "")
                + f" 保留 cluster representatives 后实际验证 {len(stable_candidates)} 个代表候选，压制 {len([r for r in representative_notes if r.get('suppressed_into')])} 个重复/近重复候选。"
                + (f" analyst 核心候选命中 {', '.join(sorted(analyst_core & set(stable_candidates)))}。" if analyst_core & set(stable_candidates) else "")
            )
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
            append_task(task, 'stable_validation_already_covered')
    representative_competition_pairs = [row for row in representative_notes if row.get('suppressed_into')]
    if stable_candidates and representative_competition_pairs:
        representative_focus = sorted({row.get('suppressed_into') for row in representative_competition_pairs if row.get('suppressed_into')})[:3]
        task = make_task(
            task_type='diagnostic',
            category='validation',
            priority_hint=26,
            reason='代表因子需要专项验证，确认被保留的 representative 确实优于同簇被压制变体。',
            expected_knowledge_gain=['representative_candidate_confirmed', 'search_space_reduced'],
            payload={
                'diagnostic_type': 'representative_candidate_competition_review',
                'focus_factors': representative_focus,
                'reasons': ['cluster_representative_competition'],
                'source_output_dir': 'artifacts/tushare_batch',
            },
            worker_note='validation｜代表因子专项验证',
            goal='validate_representative_candidates',
            hypothesis='同簇中被保留的 representative 候选应比被压制变体更有持续研究价值。',
            branch_id='representative_candidate_competition',
            stop_if=['representative_candidate_competition_shows_no_incremental_advantage'],
            promote_if=['representative_candidate_confirmed'],
            disconfirm_if=['suppressed_variant_outperforms_representative'],
        )
        task['focus_candidates'] = [_attach_quality_context(candidate_context_by_name.get(name, {}), quality_map, failure_map) for name in representative_focus]
        for row in task['focus_candidates']:
            if row:
                row['evidence_gate'] = _candidate_evidence_gate(row, evidence_policy)
        task['relationship_signal'] = {
            'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
            'cluster_competition_count': len(representative_competition_pairs),
            'representative_review_count': int(representative_candidate_stats.get('count') or 0),
            'representative_gain_count': int(representative_candidate_stats.get('gain_count') or 0),
        }
        if representative_candidate_stats.get('recommended_action') == 'upweight':
            task['priority_hint'] = max(1, int(task.get('priority_hint', 26)) - 4)
            task['reason'] += ' 既往代表因子专项验证已出现增益，本轮继续加权。'
        elif representative_candidate_stats.get('recommended_action') == 'downweight':
            task['priority_hint'] = int(task.get('priority_hint', 26)) + 4
            task['reason'] += ' 既往代表因子专项验证重复低增益，本轮适度降权。'
        append_task(task, 'representative_competition_already_covered')

    if graveyard_level and ('graveyard_diagnosis' in selected_families or not selected_families):
        graveyard_tasks = build_graveyard_task(graveyard_level, latest_graveyard, existing_fingerprints)
        for task in graveyard_tasks:
            task['relationship_signal'] = {
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'same_family_count': int(relationship_summary.get('same_family', 0)),
            }
            if analyst_graveyard & set(latest_graveyard):
                task['priority_hint'] -= 5
            task['analyst_alignment'] = {
                'focus_overlap': sorted(analyst_focus & set(latest_graveyard)),
                'core_overlap': sorted(analyst_core & set(latest_graveyard)),
                'graveyard_overlap': sorted(analyst_graveyard & set(latest_graveyard)),
            }
            task['reason'] += f" duplicate={relationship_summary.get('duplicate_of', 0)}、same_family={relationship_summary.get('same_family', 0)}，可检查 graveyard 是否集中出现在同构因子支路。" + (f" analyst 指定复核墓地命中 {', '.join(sorted(analyst_graveyard & set(latest_graveyard)))}。" if analyst_graveyard & set(latest_graveyard) else "")
            task['hypothesis'] = task['payload'].get('hypothesis')
            task['goal'] = task['payload'].get('goal')
            task['branch_id'] = task['payload'].get('branch_id')
            append_task(task, 'graveyard_diagnosis_already_covered')

    exploration_floor_slots = int(((quality_priority.get('exploration_floor') or {}).get('exploration_floor_slots') or 0))
    should_fill_exploration_floor = queue_budget.get('exploration', 0) < exploration_floor_slots
    if (should_fill_exploration_floor or (not exploration_state.get('should_throttle') and queue_budget.get('exploration', 0) < 1)):
        generated_batch_path = ROOT / 'artifacts' / 'generated_batch_from_llm.json'
        if generated_batch_path.exists() and not quality_priority['freeze_exploration']:
            task = make_task(
                task_type='generated_batch',
                category='exploration',
                priority_hint=55,
                reason='当前 exploration 未被 throttle，可允许一个受控生成 batch 进入候选池。',
                expected_knowledge_gain=['exploration_candidate_survived', 'exploration_graveyard_identified'],
                payload={'batch_path': str(generated_batch_path.relative_to(ROOT)), 'output_dir': 'artifacts/llm_generated_batch_run'},
                worker_note='exploration｜执行 LLM 生成的 batch',
                goal='explore_new_candidate_branch',
                hypothesis='受控 exploration 可能发现能补充现有 family 结构的新候选，而不是重复旧信号。',
                branch_id='exploration_generated_batch',
                stop_if=['exploration_produces_no_information_gain_twice'],
                promote_if=['exploration_surfaces_new_candidate_family'],
                disconfirm_if=['exploration_only_repeats_existing_graveyard_patterns'],
            )
            task['relationship_signal'] = {
                'hybrid_count': int(relationship_summary.get('hybrid_of', 0)),
                'top_family_score': max([row.get('family_score') or 0 for row in family_score_map.values()] or [0]),
                'quality_priority_mode': quality_priority['quality_priority_mode'],
            }
            if must_validate_before_expand:
                task['priority_hint'] += 12
            if frontier_focus.get('preferred_families'):
                task['priority_hint'] -= 3
                task['payload']['frontier_focus'] = {
                    'preferred_families': frontier_focus.get('preferred_families')[:3],
                    'preferred_candidates': frontier_preferred[:4],
                    'secondary_candidates': frontier_secondary[:4],
                }
            task['reason'] += ' 当前关系图已出现 hybrid 支路，可让 exploration 有针对性地尝试跨 family 组合。' + (f" frontier 当前建议围绕 {', '.join((frontier_focus.get('preferred_families') or [])[:3])} 主线继续探索。" if frontier_focus.get('preferred_families') else '') + (' analyst 当前要求先验证风险，但仍保留探索底仓。' if must_validate_before_expand else '')
            append_task(task, 'exploration_batch_already_seen')
        elif generated_batch_path.exists():
            suppressed_tasks.append({
                'fingerprint': 'generated_batch::quality_priority_hold::artifacts/generated_batch_from_llm.json',
                'signature': 'exploration::generated_batch::quality_priority_hold',
                'worker_note': 'exploration｜执行 LLM 生成的 batch',
                'reason': 'quality_priority_hold',
                'quality_priority': quality_priority,
            })

    generated_candidate_budget = int(quality_priority.get('generated_candidate_budget') or 0)
    generated_candidate_budget_by_pool = dict(quality_priority.get('generated_candidate_budget_by_pool') or {})
    generated_tasks_by_pool = {
        OLD_SPACE_POOL: [],
        NEW_MECHANISM_POOL: [],
    }
    for task in generated_candidate_tasks:
        pool_name = (task.get('payload') or {}).get('exploration_pool') or OLD_SPACE_POOL
        generated_tasks_by_pool.setdefault(pool_name, []).append(task)

    for pool_name in (NEW_MECHANISM_POOL, OLD_SPACE_POOL):
        for task in generated_tasks_by_pool.get(pool_name, [])[: int(generated_candidate_budget_by_pool.get(pool_name) or 0)]:
            task['priority_hint'] = min(int(task.get('priority_hint') or 55), 35)
            task['reason'] += ' candidate_generation 保底配额：每轮至少保留一个新生成候选进入研究主线。'
            task['reason'] += f' 当前进入独立分池 {pool_name}。'
            if quality_priority['quality_priority_mode']:
                task['priority_hint'] += 6
                task['reason'] += f" 当前处于质量优先模式（{'; '.join(quality_priority['reasons'])}），新候选只保留低配额占位。"
            append_task(task, 'generated_candidate_duplicate_or_suppressed')
    if generated_candidate_tasks and generated_candidate_budget == 0:
        suppressed_tasks.append({
            'fingerprint': 'generated_candidate::quality_priority_hold',
            'signature': 'exploration::generated_candidate::quality_priority_hold',
            'worker_note': 'exploration｜generated_candidate:*',
            'reason': 'quality_priority_hold',
            'quality_priority': quality_priority,
        })

    sticky_medium_horizon_payload = _build_sticky_medium_horizon_payload(
        medium_focus_candidates,
        strategy_memory.get('sticky_medium_horizon_candidates') or [],
    )
    strategy_memory['sticky_medium_horizon_candidates'] = sticky_medium_horizon_payload
    strategy_memory_path.write_text(json.dumps(strategy_memory, ensure_ascii=False, indent=2), encoding='utf-8')

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
            'research_learning': research_learning,
            'research_promoter': promotion,
            'quality_priority': quality_priority,
            'approved_universe_summary': snapshot.get('approved_universe_summary') or {},
            'approved_universe_names': approved_universe_names,
            'sticky_medium_horizon_candidates': sticky_medium_horizon_payload,
            'generated_candidate_proposal_count': len(candidate_generation_plan.get('proposals') or []),
            'generated_candidate_task_count': len([t for t in candidates if (t.get('payload') or {}).get('source') == 'candidate_generation']),
            'generated_candidate_budget_by_pool': generated_candidate_budget_by_pool,
            'generated_candidate_task_count_by_pool': {
                OLD_SPACE_POOL: len([t for t in candidates if (t.get('payload') or {}).get('source') == 'candidate_generation' and (t.get('payload') or {}).get('exploration_pool') == OLD_SPACE_POOL]),
                NEW_MECHANISM_POOL: len([t for t in candidates if (t.get('payload') or {}).get('source') == 'candidate_generation' and (t.get('payload') or {}).get('exploration_pool') == NEW_MECHANISM_POOL]),
            },
        },
        'representative_selection': representative_notes,
        'suppressed_tasks': suppressed_tasks,
        'tasks': sorted(candidates, key=lambda item: (item['priority_hint'], item['category'])),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
