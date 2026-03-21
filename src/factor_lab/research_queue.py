from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.heartbeat import append_heartbeat
from factor_lab.storage import ExperimentStore
from factor_lab.workflow import run_workflow
from factor_lab.batch import run_batch
from factor_lab.change_detection import build_change_report
from factor_lab.reporting import write_sqlite_report
from factor_lab.html_report import build_html_report
from factor_lab.index_page import build_index_page
from factor_lab.summary import build_run_summary
from factor_lab.llm_feedback import summarize_generated_batch_run
from factor_lab.llm_bridge import write_bridge_status
from factor_lab.candidate_graph import build_graph_artifacts
from factor_lab.research_expansion import maybe_expand_research_space
from factor_lab.research_planner_pipeline import run_research_planner_pipeline
from factor_lab.research_runtime_state import queue_budget_snapshot, recent_failure_stats, exploration_health, parse_iso_utc, recently_finished_same_fingerprint
from factor_lab.research_strategy import update_research_memory_from_task_result
from factor_lab.opportunity_store import update_opportunity_state
from factor_lab.opportunity_evaluator import evaluate_opportunity_from_task
from datetime import datetime, timezone, timedelta
import os


BASELINE_PRIORITY = 10
VALIDATION_PRIORITY = 30
EXPLORATION_PRIORITY = 60
RETRY_PRIORITY = 15
MAX_PENDING_BASELINE = 2
MAX_PENDING_VALIDATION = 2
MAX_PENDING_EXPLORATION = 1
MAX_CONSECUTIVE_FAILURES = 3
CIRCUIT_OPEN_COOLDOWN_MINUTES = 5
EXPLORATION_NO_GAIN_THRESHOLD = 3
BASELINE_RESEED_COOLDOWN_MINUTES = 360
TASK_REPEAT_COOLDOWN_MINUTES = 180


DB_PATH = Path("artifacts") / "factor_lab.db"
STAGNATION_PATH = Path("artifacts") / "research_stagnation.json"


def _category_from_note(note: str | None) -> str:
    note = note or ""
    if note.startswith("baseline"):
        return "baseline"
    if note.startswith("validation"):
        return "validation"
    if note.startswith("exploration"):
        return "exploration"
    if note.startswith("retry"):
        return "retry"
    return "other"


def queue_budget_snapshot(store: ExperimentStore) -> dict[str, int]:
    tasks = store.list_research_tasks(limit=200)
    counts = {"baseline": 0, "validation": 0, "exploration": 0}
    for task in tasks:
        if task["status"] not in {"pending", "running"}:
            continue
        category = _category_from_note(task.get("worker_note"))
        if category in counts:
            counts[category] += 1
    return counts


def recent_failure_stats(store: ExperimentStore, limit: int = 20) -> dict[str, Any]:
    tasks = store.list_research_tasks(limit=limit)
    consecutive_failures = 0
    last_failure_at = None
    for task in tasks:
        if task["status"] == "failed":
            consecutive_failures += 1
            if last_failure_at is None:
                last_failure_at = parse_iso_utc(task.get("finished_at_utc")) or parse_iso_utc(task.get("created_at_utc"))
        elif task["status"] == "finished":
            break
    failed_recently = len([t for t in tasks[:10] if t["status"] == "failed"])
    cooldown_active = False
    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES and last_failure_at is not None:
        cooldown_active = datetime.now(timezone.utc) - last_failure_at < timedelta(minutes=CIRCUIT_OPEN_COOLDOWN_MINUTES)
    return {
        "consecutive_failures": consecutive_failures,
        "failed_recently": failed_recently,
        "last_failure_at": last_failure_at.isoformat() if last_failure_at else None,
        "cooldown_active": cooldown_active,
    }


def parse_iso_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _stagnation_state() -> dict[str, Any]:
    return _read_json(
        STAGNATION_PATH,
        {
            "consecutive_no_injection": 0,
            "last_reason": None,
            "updated_at_utc": None,
            "recovery_zero_injection_count": 0,
            "last_recovery_deadlock_at_utc": None,
        },
    )


def _bump_stagnation(*, reason: str) -> dict[str, Any]:
    state = _stagnation_state()
    state["consecutive_no_injection"] = int(state.get("consecutive_no_injection") or 0) + 1
    state["last_reason"] = reason
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    _write_json(STAGNATION_PATH, state)
    return state


def _reset_stagnation(*, reason: str) -> dict[str, Any]:
    state = {
        "consecutive_no_injection": 0,
        "last_reason": reason,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "recovery_zero_injection_count": 0,
        "last_recovery_deadlock_at_utc": None,
    }
    _write_json(STAGNATION_PATH, state)
    return state


def _bump_recovery_zero_injection() -> dict[str, Any]:
    state = _stagnation_state()
    state["recovery_zero_injection_count"] = int(state.get("recovery_zero_injection_count") or 0) + 1
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    _write_json(STAGNATION_PATH, state)
    return state


def _mark_recovery_deadlock() -> dict[str, Any]:
    state = _stagnation_state()
    state["last_recovery_deadlock_at_utc"] = datetime.now(timezone.utc).isoformat()
    state["updated_at_utc"] = state["last_recovery_deadlock_at_utc"]
    _write_json(STAGNATION_PATH, state)
    return state


def can_reseed_baseline(store: ExperimentStore) -> bool:
    tasks = store.list_research_tasks(limit=300)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=BASELINE_RESEED_COOLDOWN_MINUTES)
    for task in tasks:
        note = task.get("worker_note") or ""
        if not note.startswith("baseline"):
            continue
        finished_at = parse_iso_utc(task.get("finished_at_utc"))
        created_at = parse_iso_utc(task.get("created_at_utc"))
        latest_at = finished_at or created_at
        if latest_at and latest_at >= cutoff:
            return False
    return True


def enqueue_baseline_tasks(store: ExperimentStore) -> list[str]:
    seeds = [
        {
            "task_type": "workflow",
            "priority": BASELINE_PRIORITY,
            "config_path": "configs/tushare_workflow.json",
            "output_dir": "artifacts/tushare_workflow",
            "worker_note": "baseline｜标准中窗口基线",
        },
        {
            "task_type": "batch",
            "priority": VALIDATION_PRIORITY,
            "config_path": "configs/tushare_batch.json",
            "output_dir": "artifacts/tushare_batch",
            "worker_note": "validation｜标准 batch 对比",
        },
    ]
    budget = queue_budget_snapshot(store)
    task_ids = []
    for seed in seeds:
        category = _category_from_note(seed["worker_note"])
        if category == "baseline" and budget["baseline"] >= MAX_PENDING_BASELINE:
            continue
        if category == "validation" and budget["validation"] >= MAX_PENDING_VALIDATION:
            continue
        cfg = json.loads(Path(seed["config_path"]).read_text(encoding="utf-8"))
        fingerprint = f"{seed['task_type']}::{config_fingerprint(cfg)}::{seed['output_dir']}"
        if recently_finished_same_fingerprint(store, fingerprint):
            continue
        task_id = store.enqueue_research_task(
            task_type=seed["task_type"],
            payload={"config_path": seed["config_path"], "output_dir": seed["output_dir"]},
            priority=seed["priority"],
            fingerprint=fingerprint,
            worker_note=seed["worker_note"],
        )
        task_ids.append(task_id)
        budget[category] += 1
    return task_ids


def refresh_reports() -> None:
    write_sqlite_report(db_path=DB_PATH, output_path="artifacts/sqlite_report.md")
    build_html_report(db_path=DB_PATH, output_path="artifacts/report.html")
    build_index_page(db_path=DB_PATH, output_path="artifacts/index.html")
    build_run_summary(db_path=DB_PATH, output_path="artifacts/latest_summary.txt")
    build_change_report(db_path=DB_PATH, output_path="artifacts/change_report.md")
    build_graph_artifacts(DB_PATH, DB_PATH.parent)


def _enqueue_followups_for_workflow(store: ExperimentStore, task: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    config_path = payload["config_path"]
    followups: list[str] = []
    budget = queue_budget_snapshot(store)
    exploration_state = exploration_health(store)
    if config_path == "configs/tushare_workflow.json" and budget["validation"] < MAX_PENDING_VALIDATION:
        cfg = json.loads(Path("configs/tushare_batch.json").read_text(encoding="utf-8"))
        fingerprint = f"batch::{config_fingerprint(cfg)}::artifacts/tushare_batch"
        if not recently_finished_same_fingerprint(store, fingerprint):
            followups.append(
                store.enqueue_research_task(
                    task_type="batch",
                    payload={"config_path": "configs/tushare_batch.json", "output_dir": "artifacts/tushare_batch"},
                    priority=VALIDATION_PRIORITY,
                    fingerprint=fingerprint,
                    parent_task_id=task["task_id"],
                    worker_note="validation｜由 workflow 完成后自动触发的 batch 对比",
                )
            )
    generated_batch_path = Path("artifacts/generated_batch_from_llm.json")
    if generated_batch_path.exists() and budget["exploration"] < MAX_PENDING_EXPLORATION and not exploration_state["should_throttle"]:
        generated_batch = json.loads(generated_batch_path.read_text(encoding="utf-8"))
        fingerprint = f"generated_batch::{config_fingerprint(generated_batch)}::artifacts/llm_generated_batch_run"
        if not recently_finished_same_fingerprint(store, fingerprint):
            followups.append(
                store.enqueue_research_task(
                    task_type="generated_batch",
                    payload={
                        "batch_path": str(generated_batch_path),
                        "output_dir": "artifacts/llm_generated_batch_run",
                    },
                    priority=EXPLORATION_PRIORITY,
                    fingerprint=fingerprint,
                    parent_task_id=task["task_id"],
                    worker_note="exploration｜执行 LLM 生成的 batch",
                )
            )
    return followups


def _enqueue_followups_for_batch(store: ExperimentStore, task: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    followups: list[str] = []
    budget = queue_budget_snapshot(store)
    comparison_path = Path(payload["output_dir"]) / "batch_comparison.json"
    if not comparison_path.exists() or budget["validation"] >= MAX_PENDING_VALIDATION:
        return followups
    comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
    graveyard_presence = comparison.get("graveyard_presence", {}) or {}
    candidate_presence = comparison.get("candidate_presence", {}) or {}

    diagnostic_reasons = []
    repeated_graveyard = []
    stable_candidates = []
    if graveyard_presence:
        repeated_graveyard = [name for name, jobs in graveyard_presence.items() if len(jobs) >= 2]
        if repeated_graveyard:
            diagnostic_reasons.append(
                f"consistent_graveyard:{', '.join(sorted(repeated_graveyard[:5]))}"
            )
    if candidate_presence:
        stable_candidates = [name for name, jobs in candidate_presence.items() if len(jobs) >= 2]
        if stable_candidates:
            diagnostic_reasons.append(
                f"stable_candidates:{', '.join(sorted(stable_candidates[:5]))}"
            )

    if diagnostic_reasons:
        fingerprint = f"diagnostic::{payload['output_dir']}::{';'.join(diagnostic_reasons)}"
        if not recently_finished_same_fingerprint(store, fingerprint):
            followups.append(
                store.enqueue_research_task(
                    task_type="diagnostic",
                    payload={
                        "diagnostic_type": "batch_consistency_review",
                        "source_output_dir": payload["output_dir"],
                        "reasons": diagnostic_reasons,
                        "knowledge_gain": [
                            "stable_candidate_confirmed" if stable_candidates else None,
                            "repeated_graveyard_confirmed" if repeated_graveyard else None,
                        ],
                        "goal": "review_batch_consistency",
                        "hypothesis": "跨窗口重复出现的 stable candidate / graveyard 代表了可复用的结构信号，而不是偶然结果。",
                        "expected_information_gain": [
                            "stable_candidate_confirmed" if stable_candidates else None,
                            "repeated_graveyard_confirmed" if repeated_graveyard else None,
                        ],
                        "branch_id": "batch_consistency_review",
                        "stop_if": ["batch_consistency_review_finds_no_repeated_pattern"],
                        "promote_if": ["batch_consistency_review_confirms_repeatable_pattern"],
                        "disconfirm_if": ["batch_consistency_review_shows_inconsistent_cross_window_behavior"],
                    },
                    priority=VALIDATION_PRIORITY + 5,
                    fingerprint=fingerprint,
                    parent_task_id=task["task_id"],
                    worker_note="validation｜batch 一致性诊断",
                )
            )
    return followups


def _enqueue_followups_for_diagnostic(store: ExperimentStore, task: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    followups: list[str] = []
    budget = queue_budget_snapshot(store)
    if payload.get("diagnostic_type") == "batch_consistency_review" and budget["validation"] < MAX_PENDING_VALIDATION:
        source_output_dir = payload.get("source_output_dir")
        comparison_path = Path(source_output_dir) / "batch_comparison.json"
        if comparison_path.exists():
            comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
            repeated_graveyard = sorted((comparison.get("graveyard_presence") or {}).keys())
            if repeated_graveyard:
                fingerprint = f"diagnostic::{source_output_dir}::graveyard_neutralization::{','.join(repeated_graveyard)}"
                if not recently_finished_same_fingerprint(store, fingerprint):
                    followups.append(
                        store.enqueue_research_task(
                            task_type="diagnostic",
                            payload={
                                "diagnostic_type": "graveyard_neutralization_review",
                                "source_output_dir": source_output_dir,
                                "focus_factors": repeated_graveyard,
                                "reasons": ["repeated_graveyard_after_batch_consistency_review"],
                                "knowledge_gain": ["neutralization_diagnosis_requested"],
                                "goal": "diagnose_neutralization_failure",
                                "hypothesis": "重复进入 graveyard 的因子，可能是被 neutralization 暴露出伪 alpha 或结构性缺陷。",
                                "expected_information_gain": ["neutralization_diagnosis_requested"],
                                "branch_id": "graveyard_neutralization_review",
                                "stop_if": ["neutralization_review_finds_no_shared_failure_pattern"],
                                "promote_if": ["neutralization_review_identifies_actionable_failure_cause"],
                                "disconfirm_if": ["neutralization_effect_does_not_explain_graveyard_behavior"],
                            },
                            priority=VALIDATION_PRIORITY + 10,
                            fingerprint=fingerprint,
                            parent_task_id=task["task_id"],
                            worker_note="validation｜graveyard 中性化诊断",
                        )
                    )
    return followups


def enqueue_followup_tasks(store: ExperimentStore, task: dict[str, Any]) -> list[str]:
    payload = task["payload"]
    if task["task_type"] == "workflow":
        return _enqueue_followups_for_workflow(store, task, payload)
    if task["task_type"] == "batch":
        return _enqueue_followups_for_batch(store, task, payload)
    if task["task_type"] == "generated_batch":
        return []
    if task["task_type"] == "diagnostic":
        return _enqueue_followups_for_diagnostic(store, task, payload)
    return []


def execute_task(task: dict[str, Any]) -> str:
    payload = task["payload"]
    task_type = task["task_type"]
    if task_type == "workflow":
        run_workflow(config_path=payload["config_path"], output_dir=payload["output_dir"])
        refresh_reports()
        return f"workflow finished: {payload['config_path']}"
    if task_type == "batch":
        run_batch(config_path=payload["config_path"], output_dir=payload["output_dir"])
        refresh_reports()
        return f"batch finished: {payload['config_path']}"
    if task_type == "generated_batch":
        batch_path = Path(payload["batch_path"])
        if not batch_path.exists():
            raise FileNotFoundError(f"generated batch not found: {batch_path}")
        run_batch(str(batch_path), payload["output_dir"])
        feedback = summarize_generated_batch_run(payload["output_dir"], "artifacts/llm_plan_feedback.json")
        batch_summary = feedback.get("batch_summary", []) or []
        knowledge_gain = []
        if any((row.get("candidate_count") or 0) > 0 for row in batch_summary):
            knowledge_gain.append("exploration_candidate_survived")
        if any((row.get("graveyard_count") or 0) > 0 for row in batch_summary):
            knowledge_gain.append("exploration_graveyard_identified")
        if not knowledge_gain:
            knowledge_gain.append("no_significant_information_gain")
        write_bridge_status(
            "artifacts/llm_status.json",
            {
                "mode": "openclaw_agent_bridge",
                "status": "plan_executed",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "generated_batch_path": str(batch_path),
                "generated_batch_output_dir": payload["output_dir"],
                "feedback_path": "artifacts/llm_plan_feedback.json",
                "feedback_summary": feedback.get("batch_summary", []),
                "knowledge_gain": knowledge_gain,
            },
        )
        refresh_reports()
        return f"generated batch finished: {batch_path} | knowledge_gain={','.join(knowledge_gain)}"
    if task_type == "diagnostic":
        output_dir = Path("artifacts") / "diagnostics"
        output_dir.mkdir(parents=True, exist_ok=True)
        diagnostic_type = payload.get("diagnostic_type", "generic")
        source_output_dir = Path(payload.get("source_output_dir", "artifacts"))
        result = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "diagnostic_type": diagnostic_type,
            "source_output_dir": str(source_output_dir),
            "reasons": payload.get("reasons", []),
            "goal": payload.get("goal"),
            "hypothesis": payload.get("hypothesis"),
            "branch_id": payload.get("branch_id"),
            "expected_information_gain": payload.get("expected_information_gain", []),
            "stop_if": payload.get("stop_if", []),
            "promote_if": payload.get("promote_if", []),
            "disconfirm_if": payload.get("disconfirm_if", []),
        }
        if diagnostic_type == "batch_consistency_review":
            comparison_path = source_output_dir / "batch_comparison.json"
            comparison = json.loads(comparison_path.read_text(encoding="utf-8")) if comparison_path.exists() else {}
            result["candidate_presence"] = comparison.get("candidate_presence", {})
            result["graveyard_presence"] = comparison.get("graveyard_presence", {})
            result["representative_presence"] = comparison.get("representative_presence", {})
            stable_candidates = sorted(result["candidate_presence"].keys())
            repeated_graveyard = sorted(result["graveyard_presence"].keys())
            result["summary"] = {
                "stable_candidates": stable_candidates,
                "repeated_graveyard": repeated_graveyard,
                "hypothesis": payload.get("hypothesis") or "若某些因子跨窗口稳定落入 graveyard，应优先诊断 neutralization 或 split robustness，而不是继续盲目扩展。",
                "supports_hypothesis": bool(stable_candidates or repeated_graveyard),
                "disconfirm_conditions_hit": [] if (stable_candidates or repeated_graveyard) else payload.get("disconfirm_if", []),
                "knowledge_gain": [
                    "stable_candidate_confirmed" if stable_candidates else None,
                    "repeated_graveyard_confirmed" if repeated_graveyard else None,
                ],
            }
        out_path = output_dir / f"{task['task_id']}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return f"diagnostic finished: {diagnostic_type}"
    raise ValueError(f"unsupported task_type: {task_type}")


def _cleanup_stale_running_tasks(store: ExperimentStore, *, stale_minutes: int = 30) -> list[str]:
    tasks = store.list_research_tasks(limit=300)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
    cleaned: list[str] = []
    for task in tasks:
        if task.get("status") != "running":
            continue
        started_at = parse_iso_utc(task.get("started_at_utc")) or parse_iso_utc(task.get("created_at_utc"))
        if not started_at or started_at >= cutoff:
            continue
        # Mark stale running task as failed so it no longer blocks expansion/reseeding.
        store.finish_research_task(
            task["task_id"],
            status="failed",
            last_error="stale_running_task_cleaned",
            worker_note=((task.get("worker_note") or "") + "｜auto_cleaned_stale_running"),
        )
        cleaned.append(task["task_id"])
    return cleaned


def run_orchestrator(max_tasks: int = 1) -> dict[str, Any]:
    store = ExperimentStore(DB_PATH)
    cleaned_running = _cleanup_stale_running_tasks(store, stale_minutes=30)
    if cleaned_running:
        append_heartbeat(
            "research_orchestrator",
            "warning",
            summary=f"cleaned stale running tasks={len(cleaned_running)}",
        )
    existing_tasks = store.list_research_tasks(limit=50)
    if not existing_tasks or not any(t["status"] in {"pending", "running"} for t in existing_tasks):
        planner_result = None
        planner_error = None
        try:
            planner_result = run_research_planner_pipeline()
            append_heartbeat(
                "research_orchestrator",
                "info",
                summary=(
                    f"planner pipeline: windows={planner_result.get('registry_windows_count', 0)}, "
                    f"validation_keys={planner_result.get('registry_validation_depth_count', 0)}, "
                    f"graveyard_keys={planner_result.get('registry_graveyard_depth_count', 0)}, "
                    f"candidates={planner_result.get('candidate_count', 0)}, "
                    f"selected={planner_result.get('proposal_selected_count', 0)}, "
                    f"strategy_approved={planner_result.get('strategy_approved_count', 0)}, "
                    f"accepted={planner_result.get('validated_accepted_count', 0)}, "
                    f"recovery={planner_result.get('recovery_used', False)}, "
                    f"research_state={(planner_result.get('research_flow_state') or {}).get('state', 'unknown')}, "
                    f"tasks_injected={planner_result.get('injected_count', 0)}, "
                    f"opp_injected={((planner_result.get('opportunity_execution') or {}).get('injected_count', 0))}"
                ),
            )
        except Exception as exc:
            planner_error = str(exc)
            append_heartbeat(
                "research_orchestrator",
                "warning",
                summary=f"planner pipeline failed, fallback to rules: {planner_error}",
            )

        planner_injected = int((planner_result or {}).get("injected_count") or 0)
        opp_injected = int((((planner_result or {}).get("opportunity_execution") or {}).get("injected_count") or 0))
        injected_total = planner_injected + opp_injected
        recovery_used = bool((planner_result or {}).get("recovery_used"))

        if injected_total > 0:
            _reset_stagnation(reason="injected")
            append_heartbeat(
                "research_orchestrator",
                "info",
                summary=f"planner/opportunities injected tasks={planner_injected}+{opp_injected}",
            )
        else:
            # Recovery deadlock protection: if recovery keeps producing zero injections while the queue is empty,
            # escalate quickly into autonomous expansion / forced reseed instead of looping forever.
            if recovery_used:
                rz = _bump_recovery_zero_injection()
                append_heartbeat(
                    "research_orchestrator",
                    "warning",
                    summary=f"recovery injected 0 tasks; zero_injection_count={rz.get('recovery_zero_injection_count')}",
                )
            else:
                rz = _stagnation_state()

            # If the planner is already in recovery mode but still injected nothing,
            # don't wait for stagnation counters to accumulate: immediately try autonomous expansion.
            expanded = maybe_expand_research_space(store, max_new_tasks=(4 if recovery_used else 2), allow_repeat=recovery_used)
            if expanded:
                _reset_stagnation(reason="expanded")
                append_heartbeat("research_orchestrator", "info", summary=f"research space expanded with {len(expanded)} tasks")
            elif recovery_used and int(rz.get("recovery_zero_injection_count") or 0) >= 2:
                _mark_recovery_deadlock()
                seeded = enqueue_baseline_tasks(store)
                if seeded:
                    _reset_stagnation(reason="recovery_deadlock_reseeded")
                    append_heartbeat(
                        "research_orchestrator",
                        "stagnation_break",
                        summary=f"recovery deadlock detected; forced reseed injected {len(seeded)} baseline tasks",
                    )
                else:
                    forced = maybe_expand_research_space(store, max_new_tasks=4, allow_repeat=True)
                    if forced:
                        _reset_stagnation(reason="recovery_deadlock_forced_expand")
                        append_heartbeat(
                            "research_orchestrator",
                            "stagnation_break",
                            summary=f"recovery deadlock detected; forced expansion injected {len(forced)} tasks",
                        )
                    else:
                        st = _bump_stagnation(reason="recovery_deadlock_noop")
                        append_heartbeat(
                            "research_orchestrator",
                            "warning",
                            summary=f"recovery deadlock detected but no reseed/expansion succeeded; stagnation={st.get('consecutive_no_injection')}",
                        )
            elif can_reseed_baseline(store):
                seeded = enqueue_baseline_tasks(store)
                if seeded:
                    _reset_stagnation(reason="reseeded")
                    append_heartbeat("research_orchestrator", "info", summary=f"queue reseeded with {len(seeded)} baseline tasks")
                else:
                    st = _bump_stagnation(reason="reseed_attempt_noop")
                    append_heartbeat("research_orchestrator", "info", summary=f"queue empty; reseed noop; stagnation={st.get('consecutive_no_injection')}")
            else:
                st = _bump_stagnation(reason="reseed_cooldown_active")
                append_heartbeat("research_orchestrator", "info", summary=f"queue empty; reseed cooldown active; stagnation={st.get('consecutive_no_injection')}")

            threshold = int(os.getenv("RESEARCH_STAGNATION_THRESHOLD", "3"))
            if threshold < 1:
                threshold = 1
            force_tasks = int(os.getenv("RESEARCH_STAGNATION_FORCE_EXPAND_TASKS", "4"))
            force_tasks = max(1, min(8, force_tasks))

            st = _stagnation_state()
            if int(st.get("consecutive_no_injection") or 0) >= threshold:
                forced = maybe_expand_research_space(store, max_new_tasks=force_tasks, allow_repeat=True)
                if forced:
                    _reset_stagnation(reason="forced_expand")
                    append_heartbeat(
                        "research_orchestrator",
                        "stagnation_break",
                        summary=f"stagnation reached {threshold}, forced expansion injected {len(forced)} tasks",
                    )

    failure_state = recent_failure_stats(store)
    if failure_state["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES and failure_state["cooldown_active"]:
        append_heartbeat(
            "research_orchestrator",
            "circuit_open",
            summary=f"paused due to consecutive failures={failure_state['consecutive_failures']}",
        )
        return {
            "processed": [],
            "remaining_preview": store.list_research_tasks(limit=10),
            "guardrail": "circuit_open",
        }

    processed = []
    append_heartbeat("research_orchestrator", "started", summary=f"orchestrator awakened (max_tasks={max_tasks})")
    for _ in range(max_tasks):
        task = store.claim_next_research_task()
        if not task:
            break
        try:
            summary = execute_task(task)
            followups = enqueue_followup_tasks(store, task)
            note = summary + (f" | followups={len(followups)}" if followups else "")
            store.finish_research_task(task["task_id"], status="finished", worker_note=note)
            update_research_memory_from_task_result(
                "artifacts/research_memory.json",
                task,
                status="finished",
                summary=note,
            )
            evaluation = evaluate_opportunity_from_task(task, status="finished", summary=note)
            if evaluation:
                update_opportunity_state(evaluation["opportunity_id"], evaluation["next_state"], reason=evaluation["evaluation_label"], extra={"evaluation": evaluation})
            processed.append({"task_id": task["task_id"], "status": "finished", "summary": summary, "followup_task_ids": followups, "opportunity_evaluation": evaluation})
            append_heartbeat("research_orchestrator", "finished", summary=note, task_id=task["task_id"], task_type=task["task_type"])
        except Exception as exc:
            error_text = str(exc)

            # Deterministic config/data errors should not trip the circuit breaker.
            # Mark them as skipped (finished) and do not retry.
            if error_text.startswith("Unknown field in expression:"):
                note = f"skipped｜invalid_expression_field｜{error_text}"
                store.finish_research_task(task["task_id"], status="finished", worker_note=note)
                update_research_memory_from_task_result(
                    "artifacts/research_memory.json",
                    task,
                    status="finished",
                    summary=note,
                    error_text=error_text,
                )
                evaluation = evaluate_opportunity_from_task(task, status="failed", error_text=error_text)
                if evaluation:
                    update_opportunity_state(
                        evaluation["opportunity_id"],
                        "rejected",
                        reason="invalid_expression_field",
                        extra={"evaluation": evaluation, "error": error_text},
                    )
                processed.append({"task_id": task["task_id"], "status": "skipped", "error": error_text, "retry_task_id": None, "opportunity_evaluation": evaluation})
                append_heartbeat(
                    "research_orchestrator",
                    "skipped",
                    summary=note,
                    task_id=task["task_id"],
                    task_type=task["task_type"],
                )
                continue

            retry_task_id = None
            if (task.get("attempt_count") or 0) < 2:
                retry_fingerprint = f"retry::{task['task_id']}::{task.get('attempt_count', 0)}"
                retry_task_id = store.enqueue_research_task(
                    task_type=task["task_type"],
                    payload=task["payload"],
                    priority=RETRY_PRIORITY,
                    fingerprint=retry_fingerprint,
                    parent_task_id=task["task_id"],
                    worker_note=f"retry｜自动重试 {task['task_type']}",
                )
            store.finish_research_task(task["task_id"], status="failed", last_error=error_text)
            update_research_memory_from_task_result(
                "artifacts/research_memory.json",
                task,
                status="failed",
                error_text=error_text,
            )
            evaluation = evaluate_opportunity_from_task(task, status="failed", error_text=error_text)
            if evaluation:
                update_opportunity_state(evaluation["opportunity_id"], evaluation["next_state"], reason=evaluation["evaluation_label"], extra={"evaluation": evaluation})
            processed.append({"task_id": task["task_id"], "status": "failed", "error": error_text, "retry_task_id": retry_task_id, "opportunity_evaluation": evaluation})
            append_heartbeat("research_orchestrator", "failed", message=error_text, task_id=task["task_id"], task_type=task["task_type"], retry_task_id=retry_task_id)

    if not processed:
        append_heartbeat("research_orchestrator", "idle", summary="no pending research tasks")
    return {
        "processed": processed,
        "remaining_preview": store.list_research_tasks(limit=10),
    }
