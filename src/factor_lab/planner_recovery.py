from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_family_generators import make_task


ROOT = Path(__file__).resolve().parents[2]

RECOVERY_COOLDOWN_AFTER_NO_GAIN = 2
RECOVERY_HISTORY_WINDOW = 6


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _recovery_stats(memory: dict[str, Any], branch_id: str) -> dict[str, Any]:
    history = [row for row in (memory.get("fallback_history") or []) if row.get("branch_id") == branch_id]
    tail = history[-RECOVERY_HISTORY_WINDOW:]
    recent_no_gain = 0
    recent_success = 0
    consecutive_no_gain = 0
    for row in tail:
        if row.get("has_gain"):
            recent_success += 1
        else:
            recent_no_gain += 1
    for row in reversed(tail):
        if row.get("has_gain"):
            break
        consecutive_no_gain += 1
    cooldown_active = consecutive_no_gain >= RECOVERY_COOLDOWN_AFTER_NO_GAIN
    return {
        "history_count": len(history),
        "recent_count": len(tail),
        "recent_success": recent_success,
        "recent_no_gain": recent_no_gain,
        "consecutive_no_gain": consecutive_no_gain,
        "cooldown_active": cooldown_active,
    }


def _score_adjustment(stats: dict[str, Any]) -> int:
    delta = 0
    if stats.get("recent_success"):
        delta -= min(int(stats["recent_success"]) * 2, 6)
    if stats.get("recent_no_gain"):
        delta += min(int(stats["recent_no_gain"]) * 3, 9)
    if stats.get("consecutive_no_gain", 0) >= 2:
        delta += 6
    return delta


def build_recovery_tasks(snapshot_path: str | Path, output_path: str | Path, branch_plan_path: str | Path | None = None) -> dict[str, Any]:
    snapshot = _read_json(Path(snapshot_path), {})
    branch_plan = _read_json(Path(branch_plan_path), {}) if branch_plan_path and Path(branch_plan_path).exists() else {}
    analyst = snapshot.get("analyst_signals") or {}
    memory = _read_json(ROOT / "artifacts" / "research_memory.json", {})
    selected_families = set(branch_plan.get("selected_families") or analyst.get("suggested_families") or [])

    frontier_focus = snapshot.get("frontier_focus") or {}
    frontier_preferred = [name for name in (frontier_focus.get("preferred_candidates") or []) if name]
    frontier_suppressed = {name for name in (frontier_focus.get("suppressed_candidates") or []) if name}
    stable_candidates = frontier_preferred[:2] or [
        row.get("factor_name")
        for row in (snapshot.get("stable_candidates") or [])
        if row.get("factor_name") and row.get("factor_name") not in frontier_suppressed
    ][:2]
    latest_graveyard = [
        name for name in (snapshot.get("latest_graveyard") or [])
        if name in frontier_preferred
    ][:4]
    if not latest_graveyard and not frontier_preferred:
        latest_graveyard = [
            name for name in (snapshot.get("latest_graveyard") or [])
            if name not in frontier_suppressed
        ][:4]
    family_summary = {row.get("family"): row for row in (snapshot.get("family_summary") or []) if row.get("family")}
    candidate_context = {row.get("candidate_name"): row for row in (snapshot.get("candidate_context") or []) if row.get("candidate_name")}

    tasks: list[dict[str, Any]] = []
    recovery_reasons: list[str] = []
    suppressed_tasks: list[dict[str, Any]] = []

    stable_branch_id = "fallback_stable_candidate_validation"
    stable_stats = _recovery_stats(memory, stable_branch_id)
    if ("stable_candidate_validation" in selected_families or not selected_families) and stable_candidates:
        focus = [name for name in stable_candidates if name]
        if stable_stats["cooldown_active"]:
            suppressed_tasks.append({
                "branch_id": stable_branch_id,
                "reason": "recovery_cooldown_active",
                "stats": stable_stats,
            })
        else:
            priority_hint = 18 + _score_adjustment(stable_stats)
            task = make_task(
                "diagnostic",
                "validation",
                priority_hint,
                "主任务暂时枯竭，启动恢复动作：对核心稳定候选做一次轻量验证，帮助系统重新回到主任务流。",
                ["stable_candidate_confirmed"],
                {
                    "diagnostic_type": "fallback_stable_candidate_validation",
                    "focus_factors": focus,
                    "reasons": ["main_task_generation_exhausted", "recovery_step"],
                    "knowledge_gain": ["stable_candidate_confirmed"],
                    "source_output_dir": "artifacts/tushare_batch",
                },
                "validation｜recovery 稳定候选轻量验证",
                goal="recovery_validate_stable_candidates",
                hypothesis="当主任务暂时产不出来时，最小恢复动作应优先重新确认当前核心稳定候选。",
                branch_id=stable_branch_id,
                stop_if=["recovery_validation_no_incremental_signal_twice"],
                promote_if=["recovery_validation_confirms_core_candidates"],
                disconfirm_if=["recovery_validation_rejects_current_core_candidates"],
            )
            task["focus_candidates"] = [candidate_context[name] for name in focus if name in candidate_context]
            task["family_focus"] = "momentum" if any(name.startswith("mom") for name in focus) else "stable_candidate_validation"
            task["relationship_signal"] = {
                "lineage_count": sum(int((candidate_context.get(name) or {}).get("lineage_count") or 0) for name in focus),
                "relationship_count": sum(int((candidate_context.get(name) or {}).get("relationship_count") or 0) for name in focus),
                "family_score": max([
                    float((candidate_context.get(name) or {}).get("family_score") or 0.0) for name in focus
                ] or [0.0]),
                "recovery_recent_success": stable_stats["recent_success"],
                "recovery_recent_no_gain": stable_stats["recent_no_gain"],
            }
            task["recovery_meta"] = stable_stats
            task["reason"] += (
                f" recovery_history={stable_stats['history_count']}，recent_success={stable_stats['recent_success']}，"
                f"recent_no_gain={stable_stats['recent_no_gain']}，consecutive_no_gain={stable_stats['consecutive_no_gain']}。"
                + (f" frontier 仅保留 {', '.join(focus)} 作为 recovery 主线。" if focus else "")
            )
            tasks.append(task)
            recovery_reasons.append("stable_candidate_validation_recovery")

    graveyard_branch_id = "fallback_graveyard_diagnosis"
    graveyard_stats = _recovery_stats(memory, graveyard_branch_id)
    if ("graveyard_diagnosis" in selected_families or analyst.get("must_validate_before_expand")) and latest_graveyard:
        focus = [name for name in latest_graveyard if name]
        if graveyard_stats["cooldown_active"]:
            suppressed_tasks.append({
                "branch_id": graveyard_branch_id,
                "reason": "recovery_cooldown_active",
                "stats": graveyard_stats,
            })
        else:
            max_risk = max([float((row.get("family_risk_score") or 0.0)) for row in family_summary.values()] or [0.0])
            priority_hint = 20 + _score_adjustment(graveyard_stats)
            task = make_task(
                "diagnostic",
                "validation",
                priority_hint,
                "主任务暂时枯竭，启动恢复动作：对当前墓地做一次轻量诊断，优先修复风险解释能力，再回到主任务流。",
                ["neutralization_diagnosis_requested"],
                {
                    "diagnostic_type": "fallback_graveyard_review",
                    "focus_factors": focus,
                    "reasons": ["research_generation_exhausted", "graveyard_review_required", "recovery_step"],
                    "knowledge_gain": ["neutralization_diagnosis_requested"],
                    "source_output_dir": "artifacts/tushare_batch",
                },
                "validation｜recovery 墓地轻量诊断",
                goal="recovery_diagnose_graveyard_failures",
                hypothesis="当主任务暂时产不出来时，应先解释当前 graveyard 与 neutralization 风险，再回到主任务推进。",
                branch_id=graveyard_branch_id,
                stop_if=["recovery_graveyard_review_no_new_signal_twice"],
                promote_if=["recovery_graveyard_review_finds_actionable_pattern"],
                disconfirm_if=["recovery_graveyard_review_shows_no_shared_failure_pattern"],
            )
            task["relationship_signal"] = {
                "duplicate_count": int((snapshot.get("relationship_summary") or {}).get("same_family", 0) or 0),
                "family_risk_score": max_risk,
                "family_recommended_action": "validate_risk" if max_risk >= 60 else "continue",
                "recovery_recent_success": graveyard_stats["recent_success"],
                "recovery_recent_no_gain": graveyard_stats["recent_no_gain"],
            }
            task["family_focus"] = "graveyard_diagnosis"
            task["recovery_meta"] = graveyard_stats
            task["reason"] += (
                f" recovery_history={graveyard_stats['history_count']}，recent_success={graveyard_stats['recent_success']}，"
                f"recent_no_gain={graveyard_stats['recent_no_gain']}，consecutive_no_gain={graveyard_stats['consecutive_no_gain']}。"
                + (f" frontier 只对仍在主线内且落入 graveyard 的候选做恢复诊断：{', '.join(focus)}。" if focus else "")
            )
            tasks.append(task)
            recovery_reasons.append("graveyard_diagnosis_recovery")

    payload = {
        "generated_from_snapshot": str(Path(snapshot_path)),
        "generated_from_branch_plan": str(branch_plan_path) if branch_plan_path else None,
        "summary": {
            "candidate_count": len(tasks),
            "recovery_used": True,
            "recovery_reasons": recovery_reasons,
            "suppressed_count": len(suppressed_tasks),
        },
        "tasks": sorted(tasks, key=lambda item: (item["priority_hint"], item["worker_note"])),
        "suppressed_tasks": suppressed_tasks,
        "recovery_used": True,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
