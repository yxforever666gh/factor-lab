from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ResearchBranchPlanner:
    def plan(self, space_map: dict[str, Any], snapshot: dict[str, Any], candidate_pool: dict[str, Any] | None = None) -> dict[str, Any]:
        family_progress = space_map.get("family_progress", {}) or {}
        fatigue = space_map.get("family_fatigue", {}) or {}
        saturation = space_map.get("family_saturation", {}) or {}
        family_recent_gain = space_map.get("family_recent_gain", {}) or {}
        candidate_tasks = (candidate_pool or {}).get("tasks", []) or []
        family_summary = snapshot.get("family_summary", []) or []
        relationship_summary = snapshot.get("relationship_summary", {}) or {}

        top_family_score = max([row.get("family_score") or 0 for row in family_summary] or [0])
        hybrid_count = int(relationship_summary.get("hybrid_of", 0) or 0)
        refinement_count = int(relationship_summary.get("refinement_of", 0) or 0)
        duplicate_count = int(relationship_summary.get("duplicate_of", 0) or 0)

        branch_decisions = []
        priority_scored: list[tuple[float, str]] = []

        for family in ["stable_candidate_validation", "graveyard_diagnosis", "recent_window_validation", "window_expansion", "exploration"]:
            progress = family_progress.get(family, {}) or {}
            next_level = progress.get("next_level")
            saturated = (saturation.get(family) or {}).get("saturated", False)
            fatigue_level = (fatigue.get(family) or {}).get("fatigue_level", "low")
            recent_gain = family_recent_gain.get(family, 0)
            score = 0.0

            if saturated or next_level is None:
                branch_decisions.append({
                    "family": family,
                    "decision": "pause",
                    "reason": "当前 family 已无下一层或已饱和。",
                })
                continue

            if family == "stable_candidate_validation":
                score += 70 + min(top_family_score / 4, 30) + min(refinement_count * 4, 12)
                decision = "advance"
                reason = f"高分 family={top_family_score:.2f}，refinement={refinement_count}，优先把强主线做深。最近增量 {recent_gain}。"
            elif family == "graveyard_diagnosis":
                score += 52 + min(duplicate_count * 5, 20)
                decision = "advance"
                reason = f"duplicate={duplicate_count}，需要确认失败因子是否只是同构重复。最近增量 {recent_gain}。"
            elif family == "recent_window_validation":
                score += 60 + min(refinement_count * 3, 12)
                decision = "advance"
                reason = f"近期窗口验证仍有缺口，且 refinement={refinement_count}，适合先确认分支稳定性。最近增量 {recent_gain}。"
            elif family == "window_expansion":
                score += 48 + min(hybrid_count * 4, 16) + min(top_family_score / 8, 10)
                decision = "advance"
                reason = f"hybrid={hybrid_count}，需要跨更长区间确认组合关系是否持久。最近增量 {recent_gain}。"
            else:
                score += 35 + min(hybrid_count * 3, 12)
                decision = "hold" if fatigue_level != "low" or top_family_score < 70 else "advance"
                reason = f"exploration 仅在已有 family 分数较强且混合支路出现时推进。最近增量 {recent_gain}。"

            if fatigue_level == "medium":
                score -= 6
            elif fatigue_level == "high":
                score -= 14

            if recent_gain:
                score += min(recent_gain * 2, 8)

            if decision == "advance":
                priority_scored.append((score, family))

            branch_decisions.append({
                "family": family,
                "decision": decision,
                "current_level": progress.get("current_level"),
                "next_level": next_level,
                "fatigue": fatigue_level,
                "priority_score": round(score, 3),
                "reason": reason,
            })

        selected_tasks = []
        selected_families = [family for _, family in sorted(priority_scored, key=lambda item: (-item[0], item[1]))]
        for family in selected_families:
            family_matches = []
            for task in candidate_tasks:
                worker_note = task.get("worker_note", "")
                if family == "stable_candidate_validation" and "稳定候选" in worker_note:
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
            "summary": "优先推进高分 family 的验证与带 lineage 的分支，再决定扩窗与 exploration。",
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
