from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _normalize_decision(action_hint: str | None, default: str = "hold") -> str:
    mapping = {
        "continue": "advance",
        "refine": "refine",
        "pause": "pause",
        "explore_new_branch": "advance",
        "validate_risk": "advance",
    }
    return mapping.get(action_hint or "", default)


class ResearchBranchPlanner:
    def plan(self, space_map: dict[str, Any], snapshot: dict[str, Any], candidate_pool: dict[str, Any] | None = None) -> dict[str, Any]:
        family_progress = space_map.get("family_progress", {}) or {}
        fatigue = space_map.get("family_fatigue", {}) or {}
        saturation = space_map.get("family_saturation", {}) or {}
        family_recent_gain = space_map.get("family_recent_gain", {}) or {}
        candidate_tasks = (candidate_pool or {}).get("tasks", []) or []
        family_summary = snapshot.get("family_summary", []) or []
        available_task_families: set[str] = set()
        for task in candidate_tasks:
            worker_note = task.get("worker_note", "") or ""
            if "稳定候选" in worker_note:
                available_task_families.add("stable_candidate_validation")
            elif "中窗" in worker_note:
                available_task_families.add("medium_horizon_validation")
            elif "graveyard" in worker_note:
                available_task_families.add("graveyard_diagnosis")
            elif "近期" in worker_note:
                available_task_families.add("recent_window_validation")
            elif "扩窗" in worker_note or "expanding" in worker_note:
                available_task_families.add("window_expansion")
            elif "exploration" in worker_note:
                available_task_families.add("exploration")
        relationship_summary = snapshot.get("relationship_summary", {}) or {}
        family_recommendations = {row.get("family"): row for row in snapshot.get("family_recommendations", []) if row.get("family")}
        trial_summary = snapshot.get("research_trial_summary", {}) or {}
        analyst_signals = snapshot.get("analyst_signals") or {}
        representative_failure_dossiers = snapshot.get("representative_failure_dossiers") or {}
        failure_question_cards = snapshot.get("failure_question_cards") or []

        top_family_score = max([row.get("family_score") or 0 for row in family_summary] or [0])
        hybrid_count = int(relationship_summary.get("hybrid_of", 0) or 0)
        refinement_count = int(relationship_summary.get("refinement_of", 0) or 0)
        duplicate_count = int(relationship_summary.get("duplicate_of", 0) or 0)
        diagnose_representative_count = len([row for row in representative_failure_dossiers.values() if row.get("recommended_action") == "diagnose"])
        suppress_representative_count = len([row for row in representative_failure_dossiers.values() if row.get("recommended_action") == "suppress"])
        short_window_only_count = len([row for row in representative_failure_dossiers.values() if row.get("regime_dependency") == "short_window_only"])
        parent_delta_failure_count = len([row for row in representative_failure_dossiers.values() if row.get("parent_delta_status") == "non_incremental"])

        branch_decisions = []
        priority_scored: list[tuple[float, str]] = []

        for family in ["stable_candidate_validation", "medium_horizon_validation", "graveyard_diagnosis", "recent_window_validation", "window_expansion", "exploration"]:
            progress = family_progress.get(family, {}) or {}
            next_level = progress.get("next_level")
            saturated = (saturation.get(family) or {}).get("saturated", False)
            fatigue_level = (fatigue.get(family) or {}).get("fatigue_level", "low")
            recent_gain = family_recent_gain.get(family, 0)
            family_trial = trial_summary.get(family, {})
            trial_pressure = float(family_trial.get("trial_pressure") or 0.0)
            false_positive_pressure = float(family_trial.get("false_positive_pressure") or 0.0)
            family_rec = family_recommendations.get(family) or {}
            family_risk_score = float(family_rec.get("family_risk_score") or 0.0)
            recommended_action = family_rec.get("recommended_action")
            score = 0.0
            action_hint = None
            analyst_suggested_families = set(analyst_signals.get("suggested_families") or [])
            analyst_risk_flags = set(analyst_signals.get("risk_flags") or [])

            if family == "stable_candidate_validation":
                family_action_counts = {}
                for row in snapshot.get("family_recommendations", []) or []:
                    action = row.get("recommended_action") or "unknown"
                    family_action_counts[action] = family_action_counts.get(action, 0) + 1
                if family_action_counts:
                    action_hint = max(family_action_counts.items(), key=lambda item: item[1])[0]
                if any((row.get("recommended_action") == "validate_risk") for row in snapshot.get("family_recommendations", []) or []):
                    action_hint = "validate_risk"
            elif family == "medium_horizon_validation":
                action_hint = "validate_risk"
            elif family == "graveyard_diagnosis":
                action_hint = "refine" if duplicate_count else "continue"
            elif family == "recent_window_validation":
                action_hint = "validate_risk" if recommended_action == "validate_risk" or family_risk_score >= 60 else ("refine" if refinement_count or duplicate_count else "continue")
            elif family == "window_expansion":
                action_hint = "pause" if recommended_action == "validate_risk" or family_risk_score >= 60 else ("continue" if top_family_score >= 70 else "pause")
            elif family == "exploration":
                explore_branch_count = len([row for row in family_recommendations.values() if row.get("recommended_action") == "explore_new_branch"])
                validate_risk_count = len([row for row in family_recommendations.values() if row.get("recommended_action") == "validate_risk"])
                action_hint = "pause" if validate_risk_count else ("explore_new_branch" if explore_branch_count >= 2 else "pause")

            if next_level is None and family in available_task_families:
                next_level = 1
                saturated = False

            if saturated or next_level is None:
                branch_decisions.append({
                    "family": family,
                    "decision": "pause",
                    "recommended_action": "pause",
                    "reason": "当前 family 已无下一层或已饱和。",
                })
                continue

            if family == "stable_candidate_validation":
                score += 70 + min(top_family_score / 4, 30) + min(refinement_count * 4, 12)
                decision = _normalize_decision(action_hint, "advance")
                if action_hint == "refine":
                    score += 6
                elif action_hint == "pause":
                    score -= 10
                reason = f"高分 family={top_family_score:.2f}，refinement={refinement_count}，优先把强主线做深。最近增量 {recent_gain}。"
            elif family == "medium_horizon_validation":
                score += 66 + min(refinement_count * 2, 8) + min(short_window_only_count * 4, 12)
                decision = _normalize_decision(action_hint, "advance")
                reason = f"soft robust 候选需要跨到 60d/90d/120d 晋级赛，确认它们能否脱离短窗依赖。最近增量 {recent_gain}。"
            elif family == "graveyard_diagnosis":
                score += 52 + min(duplicate_count * 5, 20)
                decision = _normalize_decision(action_hint, "advance")
                reason = f"duplicate={duplicate_count}，需要确认失败因子是否只是同构重复。最近增量 {recent_gain}。"
            elif family == "recent_window_validation":
                score += 60 + min(refinement_count * 3, 12)
                decision = _normalize_decision(action_hint, "advance")
                reason = f"近期窗口验证仍有缺口，且 refinement={refinement_count}，适合先确认分支稳定性。最近增量 {recent_gain}。"
            elif family == "window_expansion":
                score += 48 + min(hybrid_count * 4, 16) + min(top_family_score / 8, 10)
                decision = _normalize_decision(action_hint, "pause")
                reason = f"hybrid={hybrid_count}，需要跨更长区间确认组合关系是否持久。最近增量 {recent_gain}。"
            else:
                score += 35 + min(hybrid_count * 3, 12)
                decision = _normalize_decision(action_hint, "pause")
                if fatigue_level != "low" or top_family_score < 70:
                    decision = "pause" if action_hint != "explore_new_branch" else "advance"
                if action_hint == "explore_new_branch":
                    score += 8
                if failure_question_cards:
                    score += min(len(failure_question_cards) * 3, 12)
                    decision = "advance"
                reason = f"exploration 仅在已有 family 分数较强且混合支路出现时推进。最近增量 {recent_gain}。"

            if diagnose_representative_count:
                if family in {"stable_candidate_validation", "medium_horizon_validation", "graveyard_diagnosis", "recent_window_validation"}:
                    score += min(diagnose_representative_count * 3, 9)
                elif family in {"window_expansion", "exploration"}:
                    score -= min(diagnose_representative_count * 4, 12)
                reason += f" representative_diagnose={diagnose_representative_count}。"
            if suppress_representative_count and family in {"exploration", "window_expansion"}:
                score -= min(suppress_representative_count * 4 + parent_delta_failure_count * 3, 14)
                reason += f" representative_suppress={suppress_representative_count}，parent_delta_failures={parent_delta_failure_count}。"
            if failure_question_cards and family == "exploration":
                reason += f" failure_question_cards={len(failure_question_cards)}，本轮探索应直接响应失败模式出题。"

            if fatigue_level == "medium":
                score -= 6
            elif fatigue_level == "high":
                score -= 14

            if trial_pressure >= 75:
                score -= 16
                if decision == "advance":
                    decision = "refine"
            elif trial_pressure >= 50:
                score -= 8

            if false_positive_pressure >= 75:
                score -= 18
                decision = "pause" if family != "stable_candidate_validation" else "refine"
            elif false_positive_pressure >= 45:
                score -= 8
                if decision == "advance":
                    decision = "refine"

            if family_risk_score >= 60:
                if family in {"stable_candidate_validation", "medium_horizon_validation", "recent_window_validation", "graveyard_diagnosis"}:
                    score += 10
                    decision = "advance"
                else:
                    score -= 12
                    decision = "pause"
                reason += f" family_risk_score={family_risk_score:.1f}，优先走 robustness/validation 而不是 refinement。"

            if family in analyst_suggested_families:
                score += 14
                reason += " analyst 明确建议该 family 进入本轮主线。"
            if analyst_signals.get("must_validate_before_expand") and family in {"window_expansion", "exploration"}:
                score -= 18
                decision = "pause"
                reason += " analyst 要求先验证再扩张，本轮压低扩窗/探索。"
            if "must_validate_neutralization" in analyst_risk_flags and family in {"graveyard_diagnosis", "recent_window_validation"}:
                score += 10
                reason += " analyst 标记了中性化风险，需要优先诊断。"

            if recent_gain:
                score += min(recent_gain * 2, 8)

            if decision in {"advance", "refine"}:
                priority_scored.append((score, family))

            branch_decisions.append({
                "family": family,
                "decision": decision,
                "recommended_action": action_hint,
                "current_level": progress.get("current_level"),
                "next_level": next_level,
                "fatigue": fatigue_level,
                "trial_pressure": round(trial_pressure, 6),
                "false_positive_pressure": round(false_positive_pressure, 6),
                "priority_score": round(score, 3),
                "reason": reason + f" trial_pressure={trial_pressure:.1f}，false_positive_pressure={false_positive_pressure:.1f}。",
            })

        selected_tasks = []
        selected_families = [family for _, family in sorted(priority_scored, key=lambda item: (-item[0], item[1]))]
        for family in selected_families:
            family_matches = []
            for task in candidate_tasks:
                worker_note = task.get("worker_note", "")
                if family == "stable_candidate_validation" and "稳定候选" in worker_note:
                    family_matches.append(task)
                elif family == "medium_horizon_validation" and "中窗" in worker_note:
                    family_matches.append(task)
                elif family == "graveyard_diagnosis" and "graveyard" in worker_note:
                    family_matches.append(task)
                elif family == "recent_window_validation" and "近期" in worker_note:
                    family_matches.append(task)
                elif family == "window_expansion" and ("扩窗" in worker_note or "expanding" in worker_note):
                    family_matches.append(task)
                elif family == "exploration" and "exploration" in worker_note:
                    family_matches.append(task)
            family_matches.sort(key=lambda row: (row.get("priority_hint", 999), -(row.get("relationship_signal", {}) or {}).get("lineage_count", 0)))
            if family_matches:
                selected_tasks.append(family_matches[0])
            if len(selected_tasks) >= 4:
                break

        return {
            "summary": "decision 现在直接从 recommended_action 语义映射：continue→advance、refine→refine、pause→pause、explore_new_branch→advance、validate_risk→advance；当 family / candidate 风险偏高时，优先把任务导向 robustness / validation，而不是 refinement。",
            "branch_decisions": branch_decisions,
            "selected_tasks": selected_tasks,
            "selected_families": selected_families,
        }


def build_branch_planner_output(space_map_path: str | Path, snapshot_path: str | Path, candidate_pool_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    space_map = json.loads(Path(space_map_path).read_text(encoding="utf-8"))
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    candidate_pool = json.loads(Path(candidate_pool_path).read_text(encoding="utf-8"))
    planner = ResearchBranchPlanner()
    result = planner.plan(space_map, snapshot, candidate_pool)
    payload = {
        "generated_from_space_map": str(space_map_path),
        "generated_from_snapshot": str(snapshot_path),
        "generated_from_candidate_pool": str(candidate_pool_path),
        **result,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
