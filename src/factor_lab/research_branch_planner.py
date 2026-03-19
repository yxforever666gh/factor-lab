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

        branch_decisions = []
        priority_order: list[str] = []

        for family in ["stable_candidate_validation", "graveyard_diagnosis", "recent_window_validation", "window_expansion", "exploration"]:
            progress = family_progress.get(family, {}) or {}
            next_level = progress.get("next_level")
            saturated = (saturation.get(family) or {}).get("saturated", False)
            fatigue_level = (fatigue.get(family) or {}).get("fatigue_level", "low")
            recent_gain = family_recent_gain.get(family, 0)

            if saturated or next_level is None:
                branch_decisions.append({
                    "family": family,
                    "decision": "pause",
                    "reason": "当前 family 已无下一层或已饱和。",
                })
                continue

            if family in {"stable_candidate_validation", "graveyard_diagnosis"}:
                decision = "advance"
                reason = f"当前 family 已推进到 level {progress.get('current_level', 0)}，建议继续到 level {next_level}。最近增量 {recent_gain}。"
            elif family == "recent_window_validation":
                decision = "advance"
                reason = f"近期窗口验证仍有缺口，可继续补到 level {next_level}。最近增量 {recent_gain}。"
            elif family == "window_expansion":
                decision = "advance"
                reason = f"历史窗口仍未覆盖完，可继续补到 level {next_level}。最近增量 {recent_gain}。"
            else:
                decision = "hold" if fatigue_level != "low" else "advance"
                reason = f"exploration 作为低优先级保留位，默认谨慎推进。最近增量 {recent_gain}。"

            if decision == "advance":
                priority_order.append(family)

            branch_decisions.append({
                "family": family,
                "decision": decision,
                "current_level": progress.get("current_level"),
                "next_level": next_level,
                "fatigue": fatigue_level,
                "reason": reason,
            })

        selected_tasks = []
        for family in priority_order:
            for task in candidate_tasks:
                worker_note = task.get("worker_note", "")
                if family == "stable_candidate_validation" and "稳定候选" in worker_note:
                    selected_tasks.append(task)
                    break
                if family == "graveyard_diagnosis" and "graveyard" in worker_note:
                    selected_tasks.append(task)
                    break
                if family == "recent_window_validation" and "近期" in worker_note:
                    selected_tasks.append(task)
                    break
                if family == "window_expansion" and ("扩窗" in worker_note or "expanding" in worker_note):
                    selected_tasks.append(task)
                    break
                if family == "exploration" and "exploration" in worker_note:
                    selected_tasks.append(task)
                    break
            if len(selected_tasks) >= 4:
                break

        return {
            "summary": "优先继续推进 validation 与更宽窗口，exploration 保持保守。",
            "branch_decisions": branch_decisions,
            "selected_tasks": selected_tasks,
            "selected_families": priority_order,
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
