from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"

PLANNER_BRIEF = ARTIFACTS / "planner_agent_brief.json"
FAILURE_BRIEF = ARTIFACTS / "failure_analyst_brief.json"
PLANNER_RESPONSE = ARTIFACTS / "planner_agent_response.json"
FAILURE_RESPONSE = ARTIFACTS / "failure_analyst_response.json"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _top_candidate_names(stable_candidates: list[dict[str, Any]], limit: int = 3) -> list[str]:
    names = []
    for row in stable_candidates:
        name = row.get("factor_name") if isinstance(row, dict) else row
        if name:
            names.append(name)
    return names[:limit]


def build_planner_response(brief: dict) -> dict:
    inputs = brief.get("inputs") or {}
    flow = inputs.get("research_flow_state") or {}
    failure = inputs.get("failure_state") or {}
    queue_budget = inputs.get("queue_budget") or {}
    learning = inputs.get("research_learning") or {}
    stable_candidates = inputs.get("stable_candidates") or []
    latest_graveyard = inputs.get("latest_graveyard") or []
    selected_families = list(inputs.get("branch_selected_families") or [])
    knowledge_gain_counter = inputs.get("knowledge_gain_counter") or {}
    open_questions = inputs.get("open_questions") or []
    candidate_pool_tasks = inputs.get("candidate_pool_tasks") or []
    candidate_pool_suppressed = inputs.get("candidate_pool_suppressed") or []
    candidate_hypothesis_cards = inputs.get("candidate_hypothesis_cards") or []

    stable_names = _top_candidate_names(stable_candidates, limit=3)
    stable_count = len(stable_candidates)
    recovering = flow.get("state") in {"recovering", "exhausted"}
    queue_validation = int(queue_budget.get("validation", 0) or 0)
    queue_exploration = int(queue_budget.get("exploration", 0) or 0)
    recovery_active_branch_count = int(flow.get("recovery_active_branch_count") or 0)
    no_gain_count = int(knowledge_gain_counter.get("no_significant_information_gain") or 0)
    graveyard_gain_count = int(knowledge_gain_counter.get("exploration_graveyard_identified") or 0)

    mode = "validate"
    task_mix = {"baseline": 1, "validation": 3, "exploration": 1}
    suppress_families: list[str] = []
    priority_families: list[str] = []
    recommended_actions: list[dict] = []
    rationale_bits: list[str] = []

    if recovering or recovery_active_branch_count > 0:
        mode = "recover"
        task_mix = {"baseline": 1, "validation": 2, "exploration": 0}
        rationale_bits.append("研究流仍在 recovery，先压探索，优先恢复主线。")
    elif failure.get("cooldown_active"):
        mode = "converge"
        task_mix = {"baseline": 1, "validation": 4, "exploration": 0}
        rationale_bits.append("失败冷却激活，探索预算清零，收敛优先。")
    elif stable_count >= 5 and queue_validation <= 1:
        mode = "converge"
        task_mix = {"baseline": 1, "validation": 4, "exploration": 1}
        rationale_bits.append("稳定候选充足且验证积压偏低，进入收敛期。")
    elif queue_exploration > queue_validation or no_gain_count >= 2:
        mode = "validate"
        task_mix = {"baseline": 1, "validation": 3, "exploration": 0}
        rationale_bits.append("探索空转/偏多，重新把算力拉回验证。")
    else:
        rationale_bits.append("当前处于验证主导、保留少量扩展的平衡状态。")

    family_rows = (learning.get("families") or {})
    for family, row in family_rows.items():
        if row.get("cooldown_active"):
            suppress_families.append(family)
            continue
        if row.get("recommended_action") in {"validate", "promote", "continue"}:
            priority_families.append(family)
        elif row.get("recommended_action") in {"downweight", "cooldown"}:
            suppress_families.append(family)

    for family in selected_families:
        if family not in priority_families:
            priority_families.append(family)

    if latest_graveyard or graveyard_gain_count > 0:
        priority_families.append("graveyard_diagnosis")
        recommended_actions.append({
            "type": "diagnostic",
            "target": "graveyard_diagnosis",
            "reason": "最近墓地非空或刚识别出墓地信息增益，优先解释失败共性。",
        })
    if stable_names:
        priority_families.append("stable_candidate_validation")
        recommended_actions.append({
            "type": "validation",
            "target": stable_names[0],
            "reason": "已有稳定候选，优先补跨窗口验证与晋级判断。",
        })
    if open_questions:
        recommended_actions.append({
            "type": "diagnostic",
            "target": open_questions[0],
            "reason": "存在未决研究问题，优先把问题压缩成可验证结论。",
        })
    if queue_exploration > queue_validation or recovering:
        suppress_families.append("broad_exploration")
    if candidate_pool_suppressed:
        rationale_bits.append(f"候选池已有 {len(candidate_pool_suppressed)} 个任务被压制，说明前端筛选已在收紧。")
    if not candidate_pool_tasks:
        rationale_bits.append("当前 candidate pool 较空，保留 baseline 作为再注入锚点。")

    challenger_queue = []
    hypothesis_cards = []
    for row in candidate_hypothesis_cards[:4]:
        name = row.get("candidate_name")
        if not name:
            continue
        if row.get("target_window") == "medium_horizon":
            priority_families.append("watchlist_candidate_validation")
        if row.get("incremental_value") is not None and float(row.get("incremental_value") or 0.0) >= 12:
            challenger_queue.append(name)
        hypothesis_cards.append(
            {
                "candidate_name": name,
                "mechanism_note": row.get("mechanism_note") or "候选需要结构化假设说明。",
                "target_window": row.get("target_window") or "recent_extension",
                "invalidation_signals": list(row.get("invalidation_signals") or [])[:4],
                "incremental_value_thesis": row.get("incremental_value_thesis") or f"{name} 需要证明自己不是旧 frontier 的重复变体。",
            }
        )
    if hypothesis_cards:
        rationale_bits.append(f"已为 {len(hypothesis_cards)} 个候选生成 hypothesis cards，优先验证是否具备真正增量价值。")

    payload = {
        "schema_version": "factor_lab.planner_agent_response.v1",
        "generated_at_utc": _iso_now(),
        "agent_name": "brief-runner",
        "mode": mode,
        "task_mix": task_mix,
        "priority_families": sorted(set(priority_families))[:8],
        "suppress_families": sorted(set(suppress_families))[:8],
        "recommended_actions": recommended_actions[:8],
        "hypothesis_cards": hypothesis_cards[:6],
        "challenger_queue": challenger_queue[:6],
        "confidence_score": 0.72 if recovering or stable_count >= 3 else 0.64,
        "rationale_markdown": "\n".join(f"- {x}" for x in rationale_bits[:6]),
    }
    return payload


def build_failure_response(brief: dict) -> dict:
    inputs = brief.get("inputs") or {}
    risky = inputs.get("recent_failed_or_risky_tasks") or []
    diagnostics = inputs.get("llm_diagnostics") or {}
    flow = inputs.get("research_flow_state") or {}
    latest_graveyard = inputs.get("latest_graveyard") or []
    knowledge_gain_counter = inputs.get("knowledge_gain_counter") or {}
    patterns = []
    should_stop = []
    should_probe = []
    should_reroute = []
    summary_bits = []

    if diagnostics.get("warnings"):
        patterns.append({
            "pattern_id": "diag-warning-cluster",
            "scope": "workflow",
            "symptom": ",".join(diagnostics.get("warnings") or []),
            "likely_cause": "研究流仍处恢复期，说明当前最优动作不是广撒网，而是先处理失败结构。",
            "recommended_action": "reroute",
            "confidence_score": 0.7,
        })
        should_probe.append("research_flow_state")
        should_reroute.append("broad_exploration->graveyard_diagnosis")
        summary_bits.append("系统仍在 recovering，应把探索预算转向失败解释。")

    if latest_graveyard:
        patterns.append({
            "pattern_id": "graveyard-pressure",
            "scope": "family",
            "symptom": ",".join(latest_graveyard[:3]),
            "likely_cause": "墓地候选持续出现，说明存在结构性失败模式尚未吃透。",
            "recommended_action": "diagnose",
            "confidence_score": 0.67,
        })
        should_probe.append("graveyard_diagnosis")

    if int(knowledge_gain_counter.get("no_significant_information_gain") or 0) >= 1:
        should_reroute.append("broad_exploration->stable_candidate_validation")
        summary_bits.append("近期出现无显著信息增益，探索应更聚焦。")

    for row in risky[:6]:
        note = (row.get("worker_note") or row.get("last_error") or row.get("task_type") or "unknown")
        action = "diagnose"
        if "budget_guard" in note:
            action = "deprioritize"
        elif row.get("status") in {"failed", "quarantined"}:
            action = "stop"
        patterns.append({
            "pattern_id": f"task-{row.get('task_id', 'unknown')[:8]}",
            "scope": row.get("task_type") or "workflow",
            "symptom": note,
            "likely_cause": "近期任务失败/守门，说明该路线的价值密度偏低或存在执行风险。",
            "recommended_action": action,
            "confidence_score": 0.6,
        })
        if action == "stop":
            should_stop.append(row.get("task_id") or note)
        else:
            should_probe.append(note)

    payload = {
        "schema_version": "factor_lab.failure_analyst_response.v1",
        "generated_at_utc": _iso_now(),
        "agent_name": "brief-runner",
        "failure_patterns": patterns[:10],
        "should_stop": sorted(set(x for x in should_stop if x))[:8],
        "should_probe": sorted(set(x for x in should_probe if x))[:8],
        "should_reroute": sorted(set(x for x in should_reroute if x))[:8],
        "summary_markdown": "\n".join(f"- {x}" for x in (summary_bits or ["当前优先把失败模式转成结构化诊断结论。"])),
    }
    return payload


def main() -> int:
    planner_brief = _read_json(PLANNER_BRIEF)
    failure_brief = _read_json(FAILURE_BRIEF)
    if planner_brief:
        _write_json(PLANNER_RESPONSE, build_planner_response(planner_brief))
    if failure_brief:
        _write_json(FAILURE_RESPONSE, build_failure_response(failure_brief))
    print(json.dumps({
        "ok": True,
        "planner_written": PLANNER_RESPONSE.exists(),
        "failure_written": FAILURE_RESPONSE.exists(),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
