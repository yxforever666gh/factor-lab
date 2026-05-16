from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.exploration_budget import exploration_floor_context


class ResearchPlannerAgent:
    @staticmethod
    def _budget_bucket(task: dict[str, Any]) -> str:
        category = task.get("category", "validation")
        payload = task.get("payload") or {}
        if payload.get("source") == "candidate_generation":
            return "exploration_generated"
        goal = str(task.get("goal") or payload.get("goal") or "")
        branch_id = str(task.get("branch_id") or payload.get("branch_id") or "")
        worker_note = str(task.get("worker_note") or "")
        text = " ".join([goal, branch_id, worker_note]).lower()
        if category == "validation" and ("fragile_candidate" in text or "fragile 候选" in text):
            return "validation_fragile"
        if category == "validation" and ("medium_horizon" in text or "中窗" in text):
            return "validation_medium_horizon"
        if category == "validation" and ("stable_candidate" in text or "稳定候选" in text):
            return "validation_stable"
        return category

    @staticmethod
    def _quality_row_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
        rows = ((snapshot.get("promotion_scorecard") or {}).get("rows") or [])
        return {row.get("factor_name"): row for row in rows if row.get("factor_name")}

    @staticmethod
    def _novelty_row_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
        rows = ((snapshot.get("novelty_judge") or {}).get("rows") or [])
        return {row.get("candidate_name"): row for row in rows if row.get("candidate_name")}

    @staticmethod
    def _approved_universe_names(snapshot: dict[str, Any]) -> set[str]:
        return {name for name in (snapshot.get("approved_universe_names") or []) if name}

    @staticmethod
    def _failure_analyst_maps(snapshot: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        payload = snapshot.get("failure_analyst_enhancement") or {}
        stop_map = {
            row.get("candidate_name"): row
            for row in (payload.get("stop_or_continue_recommendation") or [])
            if row.get("candidate_name")
        }
        reroute_map = {
            row.get("candidate_name"): row
            for row in (payload.get("reroute_proposals") or [])
            if row.get("candidate_name")
        }
        return stop_map, reroute_map

    @staticmethod
    def _representative_scope_key(task: dict[str, Any]) -> str | None:
        payload = task.get("payload") or {}
        focus_names = sorted(
            {
                name
                for name in (payload.get("focus_factors") or [])
                if name
            }
            | {
                row.get("candidate_name")
                for row in (task.get("focus_candidates") or [])
                if row.get("candidate_name")
            }
        )
        if not focus_names:
            return None
        return "|".join(focus_names)

    def rank_tasks(self, snapshot: dict[str, Any], candidate_pool: dict[str, Any], branch_plan: dict[str, Any] | None = None) -> dict[str, Any]:
        tasks = list(candidate_pool.get("tasks", []))
        exploration_state = (snapshot.get("exploration_state") or {})
        failure_state = (snapshot.get("failure_state") or {})
        knowledge_gain_counter = snapshot.get("knowledge_gain_counter") or {}
        selected_families = set((branch_plan or {}).get("selected_families", []))
        quality_map = self._quality_row_map(snapshot)
        novelty_map = self._novelty_row_map(snapshot)
        approved_universe_names = self._approved_universe_names(snapshot)
        failure_stop_map, failure_reroute_map = self._failure_analyst_maps(snapshot)
        analyst_signals = snapshot.get("analyst_signals") or {}
        analyst_focus = set(analyst_signals.get("focus_factors") or [])
        analyst_core = set(analyst_signals.get("keep_as_core_candidates") or [])
        analyst_graveyard = set(analyst_signals.get("review_graveyard") or [])

        ranked = []
        for task in tasks:
            score = 100 - int(task.get("priority_hint", 50))
            reason_bits = [task.get("reason", "")]
            category = task.get("category")
            expected = set(task.get("expected_knowledge_gain", []))
            worker_note = task.get("worker_note", "")
            relationship_signal = task.get("relationship_signal", {}) or {}
            family_focus = task.get("family_focus")
            focus_candidate_rows = list(task.get("focus_candidates") or [])
            focus_candidates = {row.get("candidate_name") for row in focus_candidate_rows if row.get("candidate_name")}
            quality_rows = [quality_map.get(name) for name in focus_candidates if quality_map.get(name)]
            novelty_rows = [novelty_map.get(name) for name in focus_candidates if novelty_map.get(name)]
            evidence_rows = [row.get("evidence_gate") or {} for row in focus_candidate_rows if row.get("evidence_gate")]
            dossier_rows = [row.get("failure_dossier") or {} for row in focus_candidate_rows if row.get("failure_dossier")]
            evidence_missing = any(row.get("action") == "evidence_missing" for row in evidence_rows)
            evidence_needs_validation = any(row.get("action") == "needs_validation" for row in evidence_rows)
            approved_focus = focus_candidates & approved_universe_names
            approved_alignment = task.get("approved_universe_alignment") or {}
            approved_states = set(approved_alignment.get("state_summary") or [])
            approved_budget_weight = float(approved_alignment.get("budget_weight") or 0.0)
            governance_actions = set(approved_alignment.get("governance_actions") or [])
            failure_stop_hits = [name for name in focus_candidates if (failure_stop_map.get(name) or {}).get("recommendation") == "stop"]
            failure_reroute_hits = [name for name in focus_candidates if failure_reroute_map.get(name)]

            if approved_focus:
                if "rejected" in approved_states:
                    if category in {"exploration", "baseline"}:
                        score -= 25
                        reason_bits.append(f"approved universe: 命中已 rejected 候选 {','.join(sorted(approved_focus))}，避免继续消耗主线预算。")
                    else:
                        score += 2
                        reason_bits.append("approved universe: 已被拒绝的候选只保留轻量复核/诊断。")
                elif category == "validation":
                    score += 18
                    if "approved" in approved_states:
                        score += min(int(approved_budget_weight * 20), 8)
                    elif "watchlist" in approved_states:
                        score += 6
                    reason_bits.append(f"approved universe: 命中 {','.join(sorted(approved_focus))}，state={','.join(sorted(approved_states)) or 'approved'}，优先补证据/维护入池候选。")
                elif category == "exploration":
                    if "shadow" in approved_states or governance_actions & {"monitor_negative_contribution", "demote_bridge_candidate", "demote_candidate"}:
                        score -= 10
                        reason_bits.append("approved universe: 当前处于 watch/shadow/demotion 监控，探索降权。")
                    else:
                        score += 4
                        reason_bits.append(f"approved universe: exploration 与入池候选相关，保留轻量探索。")

            if category == "validation":
                score += 12
                reason_bits.append("当前优先补验证深度，避免只拓宽时间窗口。")
                if evidence_missing or evidence_needs_validation:
                    score += 10
                    reason_bits.append("证据完整性不足，优先补验证而不是提前晋升。")
            if failure_stop_hits:
                if category in {"exploration", "baseline"}:
                    score -= 22
                else:
                    score += 4
                reason_bits.append(f"failure_analyst: stop={','.join(sorted(failure_stop_hits))}。")
            if failure_reroute_hits:
                if category == "validation":
                    score += 8
                elif category == "exploration":
                    score -= 8
                reason_bits.append(f"failure_analyst: reroute={','.join(sorted(failure_reroute_hits))}。")
            if dossier_rows:
                diagnose_needed = len([row for row in dossier_rows if row.get("recommended_action") == "diagnose"])
                suppress_needed = len([row for row in dossier_rows if row.get("recommended_action") == "suppress"])
                parent_delta_failures = len([row for row in dossier_rows if row.get("parent_delta_status") == "non_incremental"])
                short_window_only = len([row for row in dossier_rows if row.get("regime_dependency") == "short_window_only"])
                if category == "validation":
                    score += min(diagnose_needed * 8, 16)
                    if short_window_only:
                        score += min(short_window_only * 4, 8)
                    reason_bits.append(f"failure_dossier: diagnose={diagnose_needed}, short_window_only={short_window_only}。")
                elif category == "exploration" and (suppress_needed or parent_delta_failures):
                    score -= min(suppress_needed * 12 + parent_delta_failures * 6, 22)
                    reason_bits.append(f"failure_dossier: suppress={suppress_needed}, non_incremental={parent_delta_failures}。")
            if novelty_rows:
                novelty_promote = len([row for row in novelty_rows if row.get("recommended_action") == "promote"])
                novelty_suppress = len([row for row in novelty_rows if row.get("recommended_action") == "suppress"])
                novelty_new = len([row for row in novelty_rows if row.get("novelty_class") == "new_mechanism"])
                if category == "validation":
                    score += min(novelty_promote * 6 + novelty_new * 3, 12)
                elif category == "exploration" and novelty_suppress:
                    score -= min(novelty_suppress * 10, 20)
                reason_bits.append(f"novelty_judge: promote={novelty_promote}, suppress={novelty_suppress}, new={novelty_new}。")
            if quality_rows:
                avg_quality = sum(float(row.get("quality_total_score") or 0.0) for row in quality_rows) / max(len(quality_rows), 1)
                avg_incremental = sum(float((row.get("quality_scores") or {}).get("incremental_value") or 0.0) for row in quality_rows) / max(len(quality_rows), 1)
                avg_cross_window = sum(float((row.get("quality_scores") or {}).get("cross_window_robustness") or 0.0) for row in quality_rows) / max(len(quality_rows), 1)
                avg_neutralized = sum(float((row.get("quality_scores") or {}).get("neutralized_quality") or 0.0) for row in quality_rows) / max(len(quality_rows), 1)
                avg_independence = sum(float((row.get("quality_scores") or {}).get("deduped_independence") or 0.0) for row in quality_rows) / max(len(quality_rows), 1)
                score += min(avg_quality / 8.0, 10)
                if category == "validation":
                    score += min(avg_incremental / 4.0, 5)
                    score += min(avg_cross_window / 6.0, 5)
                    score += min(avg_neutralized / 6.0, 4)
                if category == "exploration" and avg_independence <= 4:
                    score -= 10
                    reason_bits.append("quality objective: 去重后独立性偏低，exploration 降权。")
                if any((row.get("quality_classification") == "duplicate-suppress") for row in quality_rows) and category == "exploration":
                    score -= 18
                    reason_bits.append("quality objective: 命中 duplicate-suppress，exploration 强降权。")
                if any((row.get("quality_classification") in {"needs-validation", "stable-alpha-candidate"}) for row in quality_rows) and category == "validation":
                    score += 8
                    reason_bits.append("quality objective: 命中高质量候选，validation 加权。")
                reason_bits.append(f"quality_total≈{avg_quality:.1f}, incremental≈{avg_incremental:.1f}, cross_window≈{avg_cross_window:.1f}。")
            if category == "baseline":
                score += 8
                reason_bits.append("当前历史窗口仍有拓宽空间。")
            if category == "exploration":
                if exploration_state.get("should_throttle"):
                    score -= 30
                    reason_bits.append("exploration 当前应降权。")
                else:
                    score += 4
                    reason_bits.append("exploration 当前未被 throttle。")
                if (task.get("payload") or {}).get("source") == "candidate_generation":
                    score += 18
                    reason_bits.append("generated candidate 保底进入主线，优先验证新发明候选。")
                    triage = ((task.get("payload") or {}).get("triage") or task.get("triage") or {})
                    triage_score = float(triage.get("score") or 0.0)
                    triage_label = triage.get("label") or "medium"
                    if triage_score >= 0.67:
                        score += 12
                    elif triage_score >= 0.48:
                        score += 6
                    else:
                        score -= 6
                    reason_bits.append(f"triage={triage_label}:{triage_score:.2f}。")
                if evidence_missing:
                    score -= 8
                    reason_bits.append("证据链缺失时，exploration 不应被误判为高质量前沿。")

            if "稳定候选" in worker_note and "stable_candidate_validation" in selected_families:
                score += 20
                reason_bits.append("branch planner 已明确优先稳定候选验证主线。")
            if "graveyard" in worker_note and "graveyard_diagnosis" in selected_families:
                score += 20
                reason_bits.append("branch planner 已明确优先 graveyard 诊断主线。")
            if "近期" in worker_note and "recent_window_validation" in selected_families:
                score += 10
            if ("扩窗" in worker_note or "expanding" in worker_note) and "window_expansion" in selected_families:
                score += 10

            if "stable_candidate_validation_requested" in expected or any(x.startswith("stable_candidate_validation_v") for x in expected):
                score += 10
            if any(x.startswith("graveyard_") for x in expected):
                score += 9
            if "window_stability_check" in expected:
                score += 6

            if focus_candidates & analyst_focus:
                score += 12
                reason_bits.append(f"命中 analyst focus={','.join(sorted(focus_candidates & analyst_focus))}。")
            if focus_candidates & analyst_core:
                score += 14
                reason_bits.append(f"命中 analyst core={','.join(sorted(focus_candidates & analyst_core))}。")
            if category == "validation" and analyst_graveyard and (analyst_graveyard & set(task.get("payload", {}).get("focus_factors", []) or [])):
                score += 12
                reason_bits.append("命中 analyst 指定复核墓地。")
            if analyst_signals.get("must_validate_before_expand") and category == "exploration":
                score -= 25
                reason_bits.append("analyst 要求先验证再扩张，exploration 大幅降权。")
            if analyst_signals.get("must_validate_before_expand") and category == "baseline":
                score -= 8
                reason_bits.append("analyst 要求先验证再扩窗，baseline 略降权。")

            if knowledge_gain_counter.get("stable_candidate_confirmed", 0) > 0 and category == "validation":
                score += 5
            if knowledge_gain_counter.get("repeated_graveyard_confirmed", 0) > 0 and category == "validation":
                score += 4

            if relationship_signal.get("lineage_count"):
                score += min(int(relationship_signal["lineage_count"]) * 3, 12)
                reason_bits.append(f"lineage_count={relationship_signal['lineage_count']}，适合沿候选谱系继续推进。")
            fragile_candidate_count = int(relationship_signal.get("fragile_candidate_count") or 0)
            family_risk_score = relationship_signal.get("family_risk_score")
            family_recommended_action = relationship_signal.get("family_recommended_action")
            if fragile_candidate_count:
                if category == "validation":
                    score += min(fragile_candidate_count * 5, 15)
                    reason_bits.append(f"fragile_candidate_count={fragile_candidate_count}，先做 robustness/validation，避免过早 refinement。")
                else:
                    score -= min(fragile_candidate_count * 4, 12)
                    reason_bits.append(f"fragile_candidate_count={fragile_candidate_count}，非验证任务降权。")
            if family_risk_score is not None:
                frs = float(family_risk_score or 0.0)
                if frs >= 60:
                    if category == "validation":
                        score += 12
                        reason_bits.append(f"family_risk_score={frs:.1f}，应转向 robustness/validation。")
                    else:
                        score -= 14
                        reason_bits.append(f"family_risk_score={frs:.1f}，暂不优先 refinement / expansion。")
                elif frs >= 45 and category == "validation":
                    score += 5
                    reason_bits.append(f"family_risk_score={frs:.1f}，验证优先级上调。")
            if family_recommended_action == "validate_risk":
                if category == "validation":
                    score += 10
                else:
                    score -= 10
                reason_bits.append("family_recommended_action=validate_risk。")
            if relationship_signal.get("relationship_count"):
                score += min(int(relationship_signal["relationship_count"]) * 1.5, 8)
            if relationship_signal.get("hybrid_count") and category in {"baseline", "exploration"}:
                score += min(int(relationship_signal["hybrid_count"]) * 2, 10)
                reason_bits.append("已有 hybrid 线索，扩窗/探索都更有针对性。")
            if relationship_signal.get("duplicate_count") and category == "validation":
                score += min(int(relationship_signal["duplicate_count"]) * 2, 8)
                reason_bits.append("duplicate 关系增多，优先做去重/诊断型验证。")
            if relationship_signal.get("family_score") is not None:
                score += min(float(relationship_signal["family_score"]) / 10, 12)
                reason_bits.append(f"family_score={relationship_signal['family_score']}。")
            if relationship_signal.get("trial_pressure") is not None:
                tp = float(relationship_signal.get("trial_pressure") or 0.0)
                if tp >= 75:
                    score -= 16
                elif tp >= 50:
                    score -= 8
                elif tp <= 20:
                    score += 4
                reason_bits.append(f"trial_pressure={tp:.1f}。")
            if relationship_signal.get("false_positive_pressure") is not None:
                fp = float(relationship_signal.get("false_positive_pressure") or 0.0)
                if fp >= 75:
                    score -= 18
                elif fp >= 45:
                    score -= 9
                reason_bits.append(f"false_positive_pressure={fp:.1f}。")
            if family_focus:
                reason_bits.append(f"focus_family={family_focus}。")

            if failure_state.get("cooldown_active"):
                if category == "exploration":
                    score -= 20
                if category == "baseline":
                    score -= 5
                reason_bits.append("系统近期有失败冷却，优先保守型任务。")

            ranked.append(
                {
                    **task,
                    "planner_score": round(score, 3),
                    "planner_reason": " ".join(bit for bit in reason_bits if bit),
                }
            )

        ranked.sort(key=lambda item: (-item["planner_score"], item.get("priority_hint", 999)))

        floor = exploration_floor_context(snapshot)
        limits = {"baseline": 2, "validation": 3, "exploration": 1, "exploration_generated": 1, "validation_stable": 2, "validation_medium_horizon": 2, "validation_fragile": 1}
        if floor["true_fault_recovery"]:
            limits["exploration"] = 0
            limits["exploration_generated"] = 0
        else:
            limits["exploration"] = max(limits["exploration"], floor["exploration_floor_slots"])
            limits["exploration_generated"] = max(limits["exploration_generated"], floor["exploration_floor_slots"])

        selected = []
        counts = {"baseline": 0, "validation": 0, "exploration": 0, "exploration_generated": 0, "validation_stable": 0, "validation_medium_horizon": 0, "validation_fragile": 0}
        selected_ids = set()
        selected_representative_scopes: set[str] = set()
        exploration_reserve = 0 if floor["true_fault_recovery"] else floor["exploration_floor_slots"]
        max_total = 5 + max(0, exploration_reserve - 1)

        for task in ranked:
            if len(selected) >= max_total - exploration_reserve:
                break
            category = task.get("category", "validation")
            if category == "exploration":
                continue
            bucket = self._budget_bucket(task)
            if category in counts and counts[category] >= limits[category]:
                continue
            if bucket in counts and counts[bucket] >= limits.get(bucket, limits.get(category, 99)):
                continue
            representative_scope = self._representative_scope_key(task)
            if representative_scope and representative_scope in selected_representative_scopes:
                continue
            selected.append(task)
            selected_ids.add(id(task))
            if representative_scope:
                selected_representative_scopes.add(representative_scope)
            if category in counts:
                counts[category] += 1
            if bucket in counts:
                counts[bucket] += 1

        if exploration_reserve:
            for task in ranked:
                if len([row for row in selected if row.get("category") == "exploration"]) >= exploration_reserve:
                    break
                if id(task) in selected_ids:
                    continue
                category = task.get("category", "validation")
                if category != "exploration":
                    continue
                bucket = self._budget_bucket(task)
                if category in counts and counts[category] >= limits[category]:
                    continue
                if bucket in counts and counts[bucket] >= limits.get(bucket, limits.get(category, 99)):
                    continue
                representative_scope = self._representative_scope_key(task)
                if representative_scope and representative_scope in selected_representative_scopes:
                    continue
                selected.append(task)
                selected_ids.add(id(task))
                if representative_scope:
                    selected_representative_scopes.add(representative_scope)
                if category in counts:
                    counts[category] += 1
                if bucket in counts:
                    counts[bucket] += 1

        for task in ranked:
            if len(selected) >= max_total:
                break
            if id(task) in selected_ids:
                continue
            category = task.get("category", "validation")
            bucket = self._budget_bucket(task)
            if category in counts and counts[category] >= limits[category]:
                continue
            if bucket in counts and counts[bucket] >= limits.get(bucket, limits.get(category, 99)):
                continue
            representative_scope = self._representative_scope_key(task)
            if representative_scope and representative_scope in selected_representative_scopes:
                continue
            selected.append(task)
            selected_ids.add(id(task))
            if representative_scope:
                selected_representative_scopes.add(representative_scope)
            if category in counts:
                counts[category] += 1
            if bucket in counts:
                counts[bucket] += 1

        return {
            "summary": "优先选择带 family 分数、fragility、风险信号支撑的 validation / baseline 任务；同时保留探索底仓，除非系统处于真正故障恢复状态。",
            "recovery_used": bool(candidate_pool.get("recovery_used") or candidate_pool.get("fallback")),
            "selection_policy": {
                "max_total": max_total,
                "category_limits": limits,
                "exploration_floor": floor,
                "representative_only_budget": True,
            },
            "selected_tasks": selected,
            "rejected_tasks": [task for task in ranked if id(task) not in selected_ids],
        }


def build_research_plan(snapshot_path: str | Path, candidate_pool_path: str | Path, output_path: str | Path, branch_plan_path: str | Path | None = None) -> dict[str, Any]:
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    candidate_pool = json.loads(Path(candidate_pool_path).read_text(encoding="utf-8"))
    branch_plan = json.loads(Path(branch_plan_path).read_text(encoding="utf-8")) if branch_plan_path and Path(branch_plan_path).exists() else None
    planner = ResearchPlannerAgent()
    result = planner.rank_tasks(snapshot, candidate_pool, branch_plan)
    payload = {
        "generated_from_snapshot": str(snapshot_path),
        "generated_from_candidate_pool": str(candidate_pool_path),
        "generated_from_branch_plan": str(branch_plan_path) if branch_plan_path else None,
        **result,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
