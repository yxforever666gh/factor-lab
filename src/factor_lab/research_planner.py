from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ResearchPlannerAgent:
    def rank_tasks(self, snapshot: dict[str, Any], candidate_pool: dict[str, Any], branch_plan: dict[str, Any] | None = None) -> dict[str, Any]:
        tasks = list(candidate_pool.get("tasks", []))
        exploration_state = (snapshot.get("exploration_state") or {})
        failure_state = (snapshot.get("failure_state") or {})
        knowledge_gain_counter = snapshot.get("knowledge_gain_counter") or {}
        selected_families = set((branch_plan or {}).get("selected_families", []))

        ranked = []
        for task in tasks:
            score = 100 - int(task.get("priority_hint", 50))
            reason_bits = [task.get("reason", "")]
            category = task.get("category")
            expected = set(task.get("expected_knowledge_gain", []))
            worker_note = task.get("worker_note", "")
            relationship_signal = task.get("relationship_signal", {}) or {}
            family_focus = task.get("family_focus")

            if category == "validation":
                score += 12
                reason_bits.append("当前优先补验证深度，避免只拓宽时间窗口。")
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

            if knowledge_gain_counter.get("stable_candidate_confirmed", 0) > 0 and category == "validation":
                score += 5
            if knowledge_gain_counter.get("repeated_graveyard_confirmed", 0) > 0 and category == "validation":
                score += 4

            if relationship_signal.get("lineage_count"):
                score += min(int(relationship_signal["lineage_count"]) * 3, 12)
                reason_bits.append(f"lineage_count={relationship_signal['lineage_count']}，适合沿候选谱系继续推进。")
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

        limits = {"baseline": 2, "validation": 2, "exploration": 1}
        selected = []
        counts = {"baseline": 0, "validation": 0, "exploration": 0}
        for task in ranked:
            category = task.get("category", "validation")
            if category in counts and counts[category] >= limits[category]:
                continue
            selected.append(task)
            if category in counts:
                counts[category] += 1
            if len(selected) >= 4:
                break

        return {
            "summary": "优先选择带 family 分数与关系信号支撑的 validation / baseline 任务，exploration 保守进入。",
            "selection_policy": {
                "max_total": 4,
                "category_limits": limits,
            },
            "selected_tasks": selected,
            "rejected_tasks": [task for task in ranked if task not in selected],
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
