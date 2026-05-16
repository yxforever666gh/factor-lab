from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


REPAIR_AGENT_RESPONSE_SCHEMA_VERSION = "factor_lab.repair_agent_response.v1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cause(cause: str, confidence: float, *evidence: str) -> dict[str, Any]:
    return {
        "cause": cause,
        "confidence_score": round(max(0.0, min(1.0, float(confidence))), 2),
        "evidence": [item for item in evidence if item],
    }


def _action(action_type: str, target: str, reason: str, risk: str = "low") -> dict[str, Any]:
    return {
        "action_type": action_type,
        "target": target,
        "reason": reason,
        "risk_level": risk,
    }


def _check(check: str, success_signal: str) -> dict[str, Any]:
    return {
        "check": check,
        "success_signal": success_signal,
    }


def build_repair_response(context: dict[str, Any], *, source_label: str = "heuristic") -> dict[str, Any]:
    inputs = context.get("inputs") or {}
    stale = list(inputs.get("stale_running_candidates") or [])
    route_status = inputs.get("route_status") or {}
    heartbeat_gap = inputs.get("heartbeat_gap") or {}
    blocked_lane = inputs.get("blocked_lane_status") or {}
    queue_budget = inputs.get("queue_budget") or {}
    runtime_snapshot = inputs.get("runtime_snapshot") or {}
    queue_counts = inputs.get("queue_counts") or runtime_snapshot.get("queue_counts") or {}
    queue_liveness = inputs.get("queue_liveness") or runtime_snapshot.get("queue_liveness") or {}
    daemon_status = inputs.get("daemon_status") or runtime_snapshot.get("daemon_status") or {}
    failure_state = inputs.get("failure_state") or runtime_snapshot.get("failure_state") or {}

    incident_type = "unknown"
    severity = "low"
    repair_mode = "observe"
    suspected_root_causes: list[dict[str, Any]] = []
    recommended_actions: list[dict[str, Any]] = []
    verification_checks: list[dict[str, Any]] = []
    summary_bits: list[str] = []
    confidence = 0.55

    if stale:
        completed = [row for row in stale if row.get("outputs_complete")]
        targets = ",".join(str(row.get("task_id")) for row in stale if row.get("task_id"))
        if completed:
            incident_type = "output_state_drift"
            severity = "high"
            repair_mode = "repair"
            confidence = 0.95
            suspected_root_causes.append(
                _cause(
                    "workflow outputs were written but runtime state drifted from persisted artifacts",
                    0.95,
                    f"completed_outputs={len(completed)}",
                    completed[0].get("task_id") or "",
                    completed[0].get("output_dir") or "",
                )
            )
            recommended_actions.append(
                _action(
                    "recover_outputs_and_finalize",
                    targets,
                    "输出已经完整写出，应直接恢复 finalize，而不是继续让 zombie running 占住队列。",
                    "low",
                )
            )
            verification_checks.extend(
                [
                    _check("target tasks finish after output recovery", "all recovered task_ids become finished"),
                    _check("replacement work can proceed", "queue advances after recovered finalize"),
                ]
            )
            summary_bits.append("检测到 outputs-written-but-unfinalized 状态漂移。")
        else:
            incident_type = "stale_running"
            severity = "high"
            repair_mode = "repair"
            confidence = 0.94
            suspected_root_causes.append(
                _cause(
                    "old running task was never finalized in SQLite",
                    0.94,
                    f"stale_running_candidates={len(stale)}",
                    f"outputs_complete={len(completed)}",
                )
            )
            recommended_actions.append(
                _action(
                    "clean_stale_task",
                    targets,
                    "清理长期 running 的 zombie task，并修正 task_state / DB 状态漂移。",
                    "low",
                )
            )
            verification_checks.extend(
                [
                    _check("target tasks no longer remain in running status", "all stale task_ids leave running state"),
                    _check("replacement task can be claimed again", "new pending/running task appears or queue proceeds"),
                ]
            )
            summary_bits.append("发现 stale running task，优先做低风险清障。")
    elif route_status.get("healthy") is False:
        incident_type = "provider_route_failure"
        severity = "medium"
        repair_mode = "reroute"
        confidence = 0.82
        suspected_root_causes.append(
            _cause(
                "current provider route is unhealthy",
                0.82,
                str(route_status.get("last_error") or "route unhealthy"),
            )
        )
        recommended_actions.append(
            _action(
                "reroute_provider",
                str(route_status.get("resolved_mode") or "provider_route"),
                "当前路由不健康，应切换到更稳的 route / direct 模式。",
                "medium",
            )
        )
        verification_checks.append(_check("provider route probe succeeds again", "route_status.healthy=true"))
        summary_bits.append("检测到 provider route 不健康。")
    elif heartbeat_gap.get("available") and float(heartbeat_gap.get("seconds_since_last") or 0.0) > 900 and daemon_status.get("state") != "running":
        incident_type = "restart_loop"
        severity = "critical"
        repair_mode = "restart"
        confidence = 0.85
        suspected_root_causes.append(
            _cause(
                "daemon is not progressing and heartbeat is stale",
                0.85,
                f"seconds_since_last={heartbeat_gap.get('seconds_since_last')}",
                f"daemon_state={daemon_status.get('state')}",
            )
        )
        recommended_actions.append(
            _action(
                "restart_daemon_if_stale",
                "factor-lab-research-daemon.service",
                "daemon 长时间未更新 heartbeat，建议重启并重新验证状态文件推进。",
                "medium",
            )
        )
        verification_checks.append(_check("daemon status refreshes", "research_daemon_status.updated_at_utc becomes recent and state=running"))
        summary_bits.append("daemon/heartbeat 表现像停滞。")
    elif blocked_lane.get("only_blocked_pending") and not blocked_lane.get("healthy_lane_available"):
        incident_type = "blocked_lane_deadlock"
        severity = "high"
        repair_mode = "quarantine"
        confidence = 0.79
        suspected_root_causes.append(
            _cause(
                "only blocked pending tasks remain in queue",
                0.79,
                blocked_lane.get("summary") or "blocked lane active",
            )
        )
        recommended_actions.append(
            _action(
                "quarantine_branch",
                ",".join(blocked_lane.get("blocked_task_types") or []),
                "当前只剩 blocked lane，应隔离反复失败分支，恢复健康队列。",
                "medium",
            )
        )
        verification_checks.append(_check("healthy pending work reappears", "blocked_pending_count drops or unblocked_pending_count increases"))
        summary_bits.append("当前队列只剩 blocked lane。")
    elif queue_liveness.get("state") in {"healthy_idle", "cooldown_idle"} and not queue_liveness.get("is_queue_stall"):
        incident_type = "unknown"
        severity = "low"
        repair_mode = "observe"
        confidence = 0.72
        suspected_root_causes.append(
            _cause(
                "queue is idle but runtime liveness indicates no hard stall",
                0.72,
                f"queue_liveness={queue_liveness}",
            )
        )
        recommended_actions.append(
            _action(
                "mark_incident_only",
                "none",
                f"queue is {queue_liveness.get('state')}: {queue_liveness.get('reason')}",
                "low",
            )
        )
        verification_checks.append(_check("runtime remains healthy", "queue_liveness does not become queue_stall"))
        summary_bits.append(f"队列为空但属于 {queue_liveness.get('state')}，仅观察不补种。")
    elif queue_liveness.get("is_queue_stall") or (
        not queue_liveness and int(queue_counts.get("pending") or 0) <= 0 and int(queue_counts.get("running") or 0) <= 0
    ):
        incident_type = "queue_stall"
        severity = "medium"
        repair_mode = "repair"
        confidence = 0.73
        suspected_root_causes.append(
            _cause(
                "research queue is empty or not flowing",
                0.73,
                f"queue_budget={queue_budget}",
                f"queue_counts={queue_counts}",
                f"cooldown_active={failure_state.get('cooldown_active')}",
            )
        )
        recommended_actions.extend(
            [
                _action(
                    "reseed_queue",
                    "baseline",
                    "队列当前为空，建议注入 baseline/recovery 任务恢复流动性。",
                    "low",
                ),
                _action(
                    "refresh_runtime_snapshots",
                    "artifacts",
                    "在 queue stall 情况下刷新 runtime snapshot，避免状态文件继续陈旧。",
                    "low",
                ),
            ]
        )
        verification_checks.append(_check("new queue work is injected", "pending/running count becomes > 0"))
        summary_bits.append("当前 research queue 处于空转/空队列状态。")
    else:
        suspected_root_causes.append(_cause("no hard runtime incident detected", 0.52, "observe only"))
        recommended_actions.append(_action("mark_incident_only", "none", "当前未发现必须自动修复的硬故障，继续观察。", "low"))
        verification_checks.append(_check("runtime continues progressing", "heartbeat and last_processed keep updating"))
        summary_bits.append("当前未发现必须自动修复的硬故障。")

    return {
        "schema_version": REPAIR_AGENT_RESPONSE_SCHEMA_VERSION,
        "generated_at_utc": _iso_now(),
        "agent_name": "repair-agent-engine",
        "incident_type": incident_type,
        "severity": severity,
        "repair_mode": repair_mode,
        "suspected_root_causes": suspected_root_causes,
        "recommended_actions": recommended_actions,
        "verification_checks": verification_checks,
        "do_not_touch": ["strategy", "numeric_metrics", "promotion_gate"],
        "confidence_score": confidence,
        "summary_markdown": "\n".join(f"- {bit}" for bit in summary_bits),
        "decision_source": source_label,
        "decision_context_id": context.get("context_id"),
    }
