from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()



def build_failure_response(context: dict[str, Any], *, source_label: str = "heuristic") -> dict[str, Any]:
    inputs = context.get("inputs") or {}
    risky = inputs.get("recent_failed_or_risky_tasks") or []
    diagnostics = inputs.get("llm_diagnostics") or {}
    flow = inputs.get("research_flow_state") or {}
    latest_graveyard = inputs.get("latest_graveyard") or []
    knowledge_gain_counter = inputs.get("knowledge_gain_counter") or {}
    repair_feedback = inputs.get("repair_feedback") or {}
    patterns: list[dict[str, Any]] = []
    should_stop: list[str] = []
    should_probe: list[str] = []
    should_reroute: list[str] = []
    summary_bits: list[str] = []

    if diagnostics.get("warnings"):
        patterns.append(
            {
                "pattern_id": "diag-warning-cluster",
                "scope": "workflow",
                "symptom": ",".join(diagnostics.get("warnings") or []),
                "likely_cause": "研究流仍处恢复期，说明当前最优动作不是广撒网，而是先处理失败结构。",
                "recommended_action": "reroute",
                "confidence_score": 0.7,
            }
        )
        should_probe.append("research_flow_state")
        should_reroute.append("broad_exploration->graveyard_diagnosis")
        summary_bits.append("系统仍在 recovering，应把探索预算转向失败解释。")

    if flow.get("state") in {"recovering", "exhausted"}:
        should_reroute.append("exploration->validation")

    if latest_graveyard:
        patterns.append(
            {
                "pattern_id": "graveyard-pressure",
                "scope": "family",
                "symptom": ",".join(latest_graveyard[:3]),
                "likely_cause": "墓地候选持续出现，说明存在结构性失败模式尚未吃透。",
                "recommended_action": "diagnose",
                "confidence_score": 0.67,
            }
        )
        should_probe.append("graveyard_diagnosis")

    if int(repair_feedback.get("active_incident_count") or 0) > 0:
        patterns.append(
            {
                "pattern_id": "runtime-pressure",
                "scope": "workflow",
                "symptom": f"active_repair_incidents={repair_feedback.get('active_incident_count')}",
                "likely_cause": "运行时层当前存在 repair pressure，说明一些失败模式不是研究逻辑问题，而是环境/执行层风险。",
                "recommended_action": "reroute",
                "confidence_score": 0.71,
            }
        )
        should_probe.append("runtime_incident_review")
        for family in (repair_feedback.get("blocked_families") or []):
            if family:
                should_reroute.append(str(family))

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
        patterns.append(
            {
                "pattern_id": f"task-{str(row.get('task_id', 'unknown'))[:8]}",
                "scope": row.get("task_type") or "workflow",
                "symptom": note,
                "likely_cause": "近期任务失败/守门，说明该路线的价值密度偏低或存在执行风险。",
                "recommended_action": action,
                "confidence_score": 0.6,
            }
        )
        if action == "stop":
            should_stop.append(row.get("task_id") or note)
        else:
            should_probe.append(note)

    return {
        "schema_version": "factor_lab.failure_analyst_response.v1",
        "generated_at_utc": _iso_now(),
        "agent_name": "failure-analyst-engine",
        "failure_patterns": patterns[:10],
        "should_stop": sorted(set(x for x in should_stop if x))[:8],
        "should_probe": sorted(set(x for x in should_probe if x))[:8],
        "should_reroute": sorted(set(x for x in should_reroute if x))[:8],
        "summary_markdown": "\n".join(f"- {x}" for x in (summary_bits or ["当前优先把失败模式转成结构化诊断结论。"])) ,
        "decision_source": source_label,
        "decision_context_id": context.get("context_id"),
    }
