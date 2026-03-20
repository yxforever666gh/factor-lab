from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_family_generators import make_task


ROOT = Path(__file__).resolve().parents[2]

FALLBACK_COOLDOWN_AFTER_NO_GAIN = 2
FALLBACK_HISTORY_WINDOW = 6


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _fallback_stats(memory: dict[str, Any], branch_id: str) -> dict[str, Any]:
    history = [row for row in (memory.get("fallback_history") or []) if row.get("branch_id") == branch_id]
    tail = history[-FALLBACK_HISTORY_WINDOW:]
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
    cooldown_active = consecutive_no_gain >= FALLBACK_COOLDOWN_AFTER_NO_GAIN
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


def build_fallback_candidate_pool(snapshot_path: str | Path, output_path: str | Path, branch_plan_path: str | Path | None = None) -> dict[str, Any]:
    snapshot = _read_json(Path(snapshot_path), {})
    branch_plan = _read_json(Path(branch_plan_path), {}) if branch_plan_path and Path(branch_plan_path).exists() else {}
    analyst = snapshot.get("analyst_signals") or {}
    memory = _read_json(ROOT / "artifacts" / "research_memory.json", {})
    selected_families = set(branch_plan.get("selected_families") or analyst.get("suggested_families") or [])

    stable_candidates = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")][:2]
    latest_graveyard = list(snapshot.get("latest_graveyard") or [])[:4]
    family_summary = {row.get("family"): row for row in (snapshot.get("family_summary") or []) if row.get("family")}
    candidate_context = {row.get("candidate_name"): row for row in (snapshot.get("candidate_context") or []) if row.get("candidate_name")}

    tasks: list[dict[str, Any]] = []
    reasons: list[str] = []
    suppressed_tasks: list[dict[str, Any]] = []

    stable_branch_id = "fallback_stable_candidate_validation"
    stable_stats = _fallback_stats(memory, stable_branch_id)
    if ("stable_candidate_validation" in selected_families or not selected_families) and stable_candidates:
        focus = [name for name in stable_candidates if name]
        if stable_stats["cooldown_active"]:
            suppressed_tasks.append({
                "branch_id": stable_branch_id,
                "reason": "fallback_cooldown_active",
                "stats": stable_stats,
            })
        else:
            priority_hint = 18 + _score_adjustment(stable_stats)
            task = make_task(
                "diagnostic",
                "validation",
                priority_hint,
                "planner 候选池为空，启用受控 fallback：对核心稳定候选做一次轻量验证，避免系统空转。",
                ["stable_candidate_confirmed"],
                {
                    "diagnostic_type": "fallback_stable_candidate_validation",
                    "focus_factors": focus,
                    "reasons": ["planner_zero_candidates_fallback"],
                    "knowledge_gain": ["stable_candidate_confirmed"],
                    "source_output_dir": "artifacts/tushare_batch",
                },
                "validation｜fallback 稳定候选轻量验证",
                goal="fallback_validate_stable_candidates",
                hypothesis="在候选池被去重/冷却压空时，至少应保留一次对当前核心稳定候选的低成本验证。",
                branch_id=stable_branch_id,
                stop_if=["fallback_validation_no_incremental_signal_twice"],
                promote_if=["fallback_validation_confirms_core_candidates"],
                disconfirm_if=["fallback_validation_rejects_current_core_candidates"],
            )
            task["focus_candidates"] = [candidate_context[name] for name in focus if name in candidate_context]
            task["family_focus"] = "momentum" if any(name.startswith("mom") for name in focus) else "stable_candidate_validation"
            task["relationship_signal"] = {
                "lineage_count": sum(int((candidate_context.get(name) or {}).get("lineage_count") or 0) for name in focus),
                "relationship_count": sum(int((candidate_context.get(name) or {}).get("relationship_count") or 0) for name in focus),
                "family_score": max([
                    float((candidate_context.get(name) or {}).get("family_score") or 0.0) for name in focus
                ] or [0.0]),
                "fallback_recent_success": stable_stats["recent_success"],
                "fallback_recent_no_gain": stable_stats["recent_no_gain"],
            }
            task["fallback_meta"] = stable_stats
            task["reason"] += (
                f" fallback_history={stable_stats['history_count']}，recent_success={stable_stats['recent_success']}，"
                f"recent_no_gain={stable_stats['recent_no_gain']}，consecutive_no_gain={stable_stats['consecutive_no_gain']}。"
            )
            tasks.append(task)
            reasons.append("stable_candidate_validation_fallback")

    graveyard_branch_id = "fallback_graveyard_diagnosis"
    graveyard_stats = _fallback_stats(memory, graveyard_branch_id)
    if ("graveyard_diagnosis" in selected_families or analyst.get("must_validate_before_expand")) and latest_graveyard:
        focus = [name for name in latest_graveyard if name]
        if graveyard_stats["cooldown_active"]:
            suppressed_tasks.append({
                "branch_id": graveyard_branch_id,
                "reason": "fallback_cooldown_active",
                "stats": graveyard_stats,
            })
        else:
            max_risk = max([float((row.get("family_risk_score") or 0.0)) for row in family_summary.values()] or [0.0])
            priority_hint = 20 + _score_adjustment(graveyard_stats)
            task = make_task(
                "diagnostic",
                "validation",
                priority_hint,
                "planner 候选池为空，启用受控 fallback：对当前墓地做一次轻量诊断，避免在风险未解释前继续扩张。",
                ["neutralization_diagnosis_requested"],
                {
                    "diagnostic_type": "fallback_graveyard_review",
                    "focus_factors": focus,
                    "reasons": ["planner_zero_candidates_fallback", "graveyard_review_required"],
                    "knowledge_gain": ["neutralization_diagnosis_requested"],
                    "source_output_dir": "artifacts/tushare_batch",
                },
                "validation｜fallback 墓地轻量诊断",
                goal="fallback_diagnose_graveyard_failures",
                hypothesis="当 planner 候选被全部压制时，应优先解释当前 graveyard 与 neutralization 风险，而不是直接停摆。",
                branch_id=graveyard_branch_id,
                stop_if=["fallback_graveyard_review_no_new_signal_twice"],
                promote_if=["fallback_graveyard_review_finds_actionable_pattern"],
                disconfirm_if=["fallback_graveyard_review_shows_no_shared_failure_pattern"],
            )
            task["relationship_signal"] = {
                "duplicate_count": int((snapshot.get("relationship_summary") or {}).get("same_family", 0) or 0),
                "family_risk_score": max_risk,
                "family_recommended_action": "validate_risk" if max_risk >= 60 else "continue",
                "fallback_recent_success": graveyard_stats["recent_success"],
                "fallback_recent_no_gain": graveyard_stats["recent_no_gain"],
            }
            task["family_focus"] = "graveyard_diagnosis"
            task["fallback_meta"] = graveyard_stats
            task["reason"] += (
                f" fallback_history={graveyard_stats['history_count']}，recent_success={graveyard_stats['recent_success']}，"
                f"recent_no_gain={graveyard_stats['recent_no_gain']}，consecutive_no_gain={graveyard_stats['consecutive_no_gain']}。"
            )
            tasks.append(task)
            reasons.append("graveyard_diagnosis_fallback")

    payload = {
        "generated_from_snapshot": str(Path(snapshot_path)),
        "generated_from_branch_plan": str(branch_plan_path) if branch_plan_path else None,
        "summary": {
            "candidate_count": len(tasks),
            "fallback": True,
            "reasons": reasons,
            "suppressed_count": len(suppressed_tasks),
        },
        "tasks": sorted(tasks, key=lambda item: (item["priority_hint"], item["worker_note"])),
        "suppressed_tasks": suppressed_tasks,
        "fallback": True,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
