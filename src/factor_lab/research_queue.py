from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.heartbeat import append_heartbeat
from factor_lab.storage import ExperimentStore
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
BASELINE_RESEED_COOLDOWN_MINUTES = 30
TASK_REPEAT_COOLDOWN_MINUTES = 180
TASK_FAMILY_CIRCUIT_TYPES = ("generated_batch",)
RESOURCE_EXHAUSTION_QUARANTINE_TASK_TYPES = ("generated_batch", "workflow", "batch")


DB_PATH = Path("artifacts") / "factor_lab.db"
STAGNATION_PATH = Path("artifacts") / "research_stagnation.json"
REPORT_REFRESH_STATE_PATH = Path("artifacts") / "report_refresh_state.json"


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


def _is_maintenance_failure(task: dict[str, Any]) -> bool:
    error_text = str(task.get("last_error") or "")
    worker_note = str(task.get("worker_note") or "")
    return error_text in {"stale_running_task_cleaned", "stale_running_task_repaired_after_outputs_written"} or (
        "auto_cleaned_stale_running" in worker_note or "auto_repaired_unfinalized_workflow_output" in worker_note
    )


def recent_failure_stats(store: ExperimentStore, limit: int = 20, task_type: str | None = None) -> dict[str, Any]:
    tasks = store.list_research_tasks(limit=limit)
    consecutive_failures = 0
    last_failure_at = None
    matched_recent: list[dict[str, Any]] = []
    for task in tasks:
        if task_type and task.get("task_type") != task_type:
            continue
        matched_recent.append(task)
        if task["status"] == "failed":
            if _is_maintenance_failure(task):
                continue
            consecutive_failures += 1
            if last_failure_at is None:
                last_failure_at = parse_iso_utc(task.get("finished_at_utc")) or parse_iso_utc(task.get("created_at_utc"))
        elif task["status"] == "finished":
            break
    failed_recently = len([t for t in matched_recent[:10] if t["status"] == "failed" and not _is_maintenance_failure(t)])
    cooldown_active = False
    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES and last_failure_at is not None:
        cooldown_active = datetime.now(timezone.utc) - last_failure_at < timedelta(minutes=CIRCUIT_OPEN_COOLDOWN_MINUTES)
    return {
        "consecutive_failures": consecutive_failures,
        "failed_recently": failed_recently,
        "last_failure_at": last_failure_at.isoformat() if last_failure_at else None,
        "cooldown_active": cooldown_active,
        "task_type": task_type,
    }


def blocked_task_types(store: ExperimentStore) -> list[str]:
    blocked: list[str] = []
    for task_type in TASK_FAMILY_CIRCUIT_TYPES:
        stats = recent_failure_stats(store, limit=50, task_type=task_type)
        if stats["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES and stats["cooldown_active"]:
            blocked.append(task_type)
    return blocked


def has_pending_unblocked_tasks(store: ExperimentStore, blocked: list[str]) -> bool:
    blocked_set = set(blocked)
    for task in store.list_research_tasks(limit=200):
        if task.get("status") != "pending":
            continue
        if task.get("task_type") in blocked_set:
            continue
        return True
    return False


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


def should_refresh_reports(*, force: bool = False) -> bool:
    if force:
        return True
    cooldown_seconds = int(os.getenv("RESEARCH_REPORT_REFRESH_MIN_SECONDS", "300"))
    if cooldown_seconds <= 0:
        return True
    state = _read_json(REPORT_REFRESH_STATE_PATH, {})
    last_refresh_at = parse_iso_utc(state.get("last_refresh_at_utc"))
    if last_refresh_at is None:
        return True
    return (datetime.now(timezone.utc) - last_refresh_at).total_seconds() >= cooldown_seconds


def mark_reports_refreshed() -> None:
    _write_json(
        REPORT_REFRESH_STATE_PATH,
        {
            "last_refresh_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def refresh_reports(*, force: bool = False) -> bool:
    if not should_refresh_reports(force=force):
        return False
    write_sqlite_report(db_path=DB_PATH, output_path="artifacts/sqlite_report.md")
    build_html_report(db_path=DB_PATH, output_path="artifacts/report.html")
    build_index_page(db_path=DB_PATH, output_path="artifacts/index.html")
    build_run_summary(db_path=DB_PATH, output_path="artifacts/latest_summary.txt")
    build_change_report(db_path=DB_PATH, output_path="artifacts/change_report.md")
    build_graph_artifacts(DB_PATH, DB_PATH.parent)
    mark_reports_refreshed()
    return True


def _enqueue_generated_candidate_validation_followup(store: ExperimentStore, task: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    if not _is_generated_candidate_validation_task(task):
        return []
    stage = str(payload.get("validation_stage") or "")
    if not stage.startswith("recent_45d"):
        return []
    config_path = Path(str(payload.get("config_path") or ""))
    if not config_path.exists():
        return []
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    end_date = cfg.get("end_date")
    if not end_date:
        return []
    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except Exception:
        return []

    next_cfg = dict(cfg)
    next_cfg["start_date"] = (end_dt - timedelta(days=90)).strftime("%Y-%m-%d")
    next_output_dir = str(cfg.get("output_dir") or "").replace("recent_45d", "recent_90d")
    if next_output_dir == str(cfg.get("output_dir") or ""):
        next_output_dir = f"{next_output_dir}_recent_90d"
    next_cfg["output_dir"] = next_output_dir

    next_path = config_path.with_name(config_path.name.replace("recent_45d", "recent_90d"))
    if next_path == config_path:
        next_path = config_path.with_name(config_path.stem + "_recent_90d" + config_path.suffix)
    next_path.write_text(json.dumps(next_cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    fingerprint = f"workflow::{config_fingerprint(next_cfg)}::{next_output_dir}"
    if recently_finished_same_fingerprint(store, fingerprint):
        return []
    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={
            "config_path": str(next_path),
            "output_dir": next_output_dir,
            "source": payload.get("source") or "candidate_generation_validation",
            "validation_stage": "recent_90d_light",
        },
        priority=VALIDATION_PRIORITY + 8,
        fingerprint=fingerprint,
        parent_task_id=task["task_id"],
        worker_note=(str(task.get("worker_note") or "").replace("recent_45d", "recent_90d") or "validation｜candidate_generation recent_90d｜light"),
    )
    return [task_id]


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
    failure_state = recent_failure_stats(store)
    true_fault_recovery = bool(failure_state.get("cooldown_active"))
    should_preserve_exploration_floor = budget["exploration"] < 1 and not true_fault_recovery
    if generated_batch_path.exists() and budget["exploration"] < MAX_PENDING_EXPLORATION and (should_preserve_exploration_floor or not exploration_state["should_throttle"]):
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
    followups.extend(_enqueue_generated_candidate_validation_followup(store, task, payload))
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


def _load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _generated_config_lineage_error(config_path: Path) -> str | None:
    try:
        config = _load_json_file(config_path)
    except Exception as exc:
        return f"generated config unreadable: {config_path}: {exc}"

    factors = list(config.get("factors") or [])
    factor_map = {row.get("name"): row for row in factors if isinstance(row, dict) and row.get("name")}
    for factor in factors:
        if not isinstance(factor, dict):
            continue
        operator = factor.get("generator_operator")
        if not operator:
            continue
        left_name = factor.get("left_factor_name")
        right_name = factor.get("right_factor_name")
        if not left_name or not right_name:
            return (
                "generated config missing lineage fields: "
                f"{config_path}::{factor.get('name') or '<unnamed>'}::{operator}::{left_name},{right_name}"
            )
        if left_name not in factor_map or right_name not in factor_map:
            return (
                "generated config references missing base factor: "
                f"{config_path}::{factor.get('name') or '<unnamed>'}::{operator}::{left_name},{right_name}"
            )
    return None


def validate_generated_batch_payload(task: dict[str, Any]) -> tuple[bool, str | None]:
    payload = task.get("payload") or {}
    batch_path = Path(payload.get("batch_path") or "")
    if not batch_path.exists():
        return False, f"generated batch not found: {batch_path}"

    try:
        batch = _load_json_file(batch_path)
    except Exception as exc:
        return False, f"generated batch unreadable: {batch_path}: {exc}"

    jobs = list(batch.get("jobs") or [])
    if not jobs:
        return False, f"generated batch has no jobs: {batch_path}"

    for idx, job in enumerate(jobs):
        if not isinstance(job, dict):
            return False, f"generated batch job malformed: {batch_path}#{idx}"
        config_text = job.get("config_path")
        if not config_text:
            return False, f"generated batch job missing config_path: {batch_path}#{idx}"
        config_path = Path(config_text)
        if not config_path.exists():
            return False, f"generated batch job config missing: {config_path}"
        lineage_error = _generated_config_lineage_error(config_path)
        if lineage_error:
            return False, lineage_error
    return True, None


def classify_task_failure(task: dict[str, Any], error_text: str) -> str:
    deterministic_markers = [
        "dataset slice empty:",
        "Unknown field in expression:",
        "missing_factor_family_config",
        "generated batch not found:",
        "generated batch unreadable:",
        "generated batch has no jobs:",
        "generated batch job malformed:",
        "generated batch job missing config_path:",
        "generated batch job config missing:",
        "generated config unreadable:",
        "generated config missing lineage fields:",
        "generated config references missing base factor:",
        "missing base factor for generated operator:",
    ]
    if any(marker in error_text for marker in deterministic_markers):
        return "deterministic"
    if "factor_families" in error_text and "No such file or directory" in error_text:
        return "deterministic"
    return "transient"


def _resource_exhaustion_reason(error_text: str) -> str | None:
    if error_text.startswith("research task worker rss exceeded limit:"):
        return "worker_rss_exceeded"
    if error_text.startswith("research task worker timeout after "):
        return "worker_timeout"
    return None


def _quarantine_output_dir(task: dict[str, Any], reason: str) -> str | None:
    output_dir_text = (task.get("payload") or {}).get("output_dir")
    if not output_dir_text:
        return None
    output_dir = Path(output_dir_text)
    if not output_dir.exists():
        return None
    quarantine_root = Path("artifacts") / "quarantine"
    quarantine_root.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in output_dir.name)
    target = quarantine_root / f"{safe_name}__{reason}__{int(datetime.now(timezone.utc).timestamp())}"
    if target.exists():
        shutil.rmtree(target)
    shutil.move(str(output_dir), str(target))
    return str(target)


def _read_pid_rss_mb(pid: int) -> int:
    status_path = Path(f"/proc/{pid}/status")
    try:
        text = status_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return 0
    for line in text.splitlines():
        if not line.startswith("VmRSS:"):
            continue
        parts = line.split()
        if len(parts) < 2:
            return 0
        try:
            return int(parts[1]) // 1024
        except Exception:
            return 0
    return 0


def _execute_heavy_task_in_subprocess(task: dict[str, Any]) -> str:
    task_json = json.dumps(task, ensure_ascii=False)
    command = [
        sys.executable,
        str(Path(__file__).resolve().parents[2] / "scripts" / "run_research_task_worker.py"),
        task_json,
    ]
    task_type = str(task.get("task_type") or "").strip().lower()
    generic_timeout = int(os.getenv("RESEARCH_TASK_WORKER_TIMEOUT_SECONDS", "180"))
    timeout_defaults = {
        "workflow": 900,
        "batch": 600,
        "generated_batch": 300,
    }
    timeout_seconds = int(
        os.getenv(
            f"RESEARCH_TASK_WORKER_TIMEOUT_SECONDS_{task_type.upper()}",
            str(timeout_defaults.get(task_type, generic_timeout)),
        )
    )
    rss_limit_mb = int(os.getenv("RESEARCH_TASK_WORKER_RSS_LIMIT_MB", "2048"))
    poll_interval = float(os.getenv("RESEARCH_TASK_WORKER_POLL_SECONDS", "0.5"))
    started = time.time()
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    killed_reason = None
    while True:
        returncode = proc.poll()
        if returncode is not None:
            break
        elapsed = time.time() - started
        if timeout_seconds > 0 and elapsed >= timeout_seconds:
            killed_reason = f"research task worker timeout after {timeout_seconds}s"
            proc.kill()
            break
        if rss_limit_mb > 0:
            rss_mb = _read_pid_rss_mb(proc.pid)
            if rss_mb >= rss_limit_mb:
                killed_reason = f"research task worker rss exceeded limit: {rss_mb}MB >= {rss_limit_mb}MB"
                proc.kill()
                break
        time.sleep(max(0.1, poll_interval))

    stdout, stderr = proc.communicate()
    stdout = (stdout or "").strip()
    stderr = (stderr or "").strip()
    output = stdout or stderr
    if killed_reason:
        raise RuntimeError(killed_reason)
    if proc.returncode != 0:
        if output:
            try:
                payload = json.loads(output.splitlines()[-1])
                raise RuntimeError(payload.get("error") or payload.get("summary") or output)
            except json.JSONDecodeError:
                raise RuntimeError(output)
        raise RuntimeError(f"research task worker failed with code {proc.returncode}")

    if output:
        try:
            payload = json.loads(output.splitlines()[-1])
            if payload.get("ok"):
                return payload.get("summary") or f"{task['task_type']} finished"
            raise RuntimeError(payload.get("error") or output)
        except json.JSONDecodeError:
            return output
    return f"{task['task_type']} finished"


def execute_task(task: dict[str, Any]) -> str:
    payload = task["payload"]
    task_type = task["task_type"]
    if task_type in {"workflow", "batch", "generated_batch"}:
        return _execute_heavy_task_in_subprocess(task)
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


def _task_output_dir(task: dict[str, Any]) -> Path | None:
    payload = task.get("payload") or {}
    output_dir = payload.get("output_dir")
    if not output_dir:
        return None
    return Path(output_dir)


def _looks_like_completed_workflow_output(output_dir: Path) -> bool:
    required = [
        output_dir / "results.json",
        output_dir / "factor_scores.json",
        output_dir / "factor_graveyard.json",
        output_dir / "summary.md",
        output_dir / "portfolio_results.json",
        output_dir / "timing.json",
    ]
    return all(path.exists() for path in required)


def _looks_like_completed_batch_output(output_dir: Path) -> bool:
    return (output_dir / "batch_summary.json").exists() and (output_dir / "batch_comparison.json").exists()


def _task_outputs_look_complete(task: dict[str, Any]) -> bool:
    output_dir = _task_output_dir(task)
    if not output_dir or not output_dir.exists():
        return False
    task_type = task.get("task_type")
    if task_type == "workflow":
        return _looks_like_completed_workflow_output(output_dir)
    if task_type in {"batch", "generated_batch"}:
        return _looks_like_completed_batch_output(output_dir)
    return False


def _repair_running_task_state_file(task: dict[str, Any], *, status: str, error: str | None) -> None:
    output_dir = _task_output_dir(task)
    if not output_dir:
        return
    task_state_path = output_dir / "task_state.json"
    if not task_state_path.exists():
        return
    try:
        state = json.loads(task_state_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if state.get("status") != "running":
        return
    state["status"] = status
    state["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    state["error"] = error
    task_state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _cleanup_stale_running_tasks(store: ExperimentStore, *, stale_minutes: int = 10) -> list[str]:
    tasks = store.list_research_tasks(limit=300)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
    cleaned: list[str] = []
    for task in tasks:
        if task.get("status") != "running":
            continue
        started_at = parse_iso_utc(task.get("started_at_utc")) or parse_iso_utc(task.get("created_at_utc"))
        if not started_at or started_at >= cutoff:
            continue

        output_dir = _task_output_dir(task)
        completed_outputs = bool(output_dir and task.get("task_type") == "workflow" and _looks_like_completed_workflow_output(output_dir))
        note_suffix = "｜auto_repaired_unfinalized_workflow_output" if completed_outputs else "｜auto_cleaned_stale_running"
        error_text = "stale_running_task_repaired_after_outputs_written" if completed_outputs else "stale_running_task_cleaned"

        store.finish_research_task(
            task["task_id"],
            status="failed",
            last_error=error_text,
            worker_note=((task.get("worker_note") or "") + note_suffix),
        )
        _repair_running_task_state_file(task, status="failed", error=error_text)
        cleaned.append(task["task_id"])
    return cleaned


def _task_is_budget_risky(task: dict[str, Any]) -> bool:
    if task.get("task_type") != "generated_batch":
        return False
    payload = task.get("payload") or {}
    if (payload.get("execution_mode") or "cheap_screen") != "cheap_screen":
        return False
    if payload.get("target_candidates"):
        return False
    return (payload.get("opportunity_type") or "") == "probe"


def _is_generated_candidate_validation_task(task: dict[str, Any]) -> bool:
    if task.get("task_type") != "workflow":
        return False
    payload = task.get("payload") or {}
    config_path = str(payload.get("config_path") or "")
    worker_note = str(task.get("worker_note") or "")
    source = str(payload.get("source") or "")
    return (
        source == "candidate_generation_validation"
        or "candidate_generation" in worker_note
        or "candidate_gen__" in config_path
    )


def _task_requires_serial_execution(task: dict[str, Any]) -> bool:
    return _is_generated_candidate_validation_task(task)


def _quarantine_batch_path(task: dict[str, Any], reason: str) -> str | None:
    batch_path_text = (task.get("payload") or {}).get("batch_path")
    if not batch_path_text:
        return None
    batch_path = Path(batch_path_text)
    if not batch_path.exists():
        return None
    quarantine_root = Path("artifacts") / "quarantine" / "generated_batch_branches"
    quarantine_root.mkdir(parents=True, exist_ok=True)
    target = quarantine_root / f"{batch_path.name}__{reason}__{int(datetime.now(timezone.utc).timestamp())}"
    shutil.move(str(batch_path), str(target))
    return str(target)


def _quarantine_budget_risky_task(store: ExperimentStore, task: dict[str, Any], *, reason: str) -> None:
    moved_batch = _quarantine_batch_path(task, reason)
    moved_output = _quarantine_output_dir(task, reason)
    note = f"quarantined｜{reason}"
    if moved_batch:
        note += f"｜batch={moved_batch}"
    if moved_output:
        note += f"｜output={moved_output}"
    store.finish_research_task(
        task["task_id"],
        status="finished",
        last_error=reason,
        worker_note=note,
    )
    update_research_memory_from_task_result(
        "artifacts/research_memory.json",
        task,
        status="finished",
        summary=note,
        error_text=reason,
    )
    opportunity_id = ((task.get("payload") or {}).get("opportunity_id"))
    if opportunity_id:
        update_opportunity_state(opportunity_id, "rejected", reason=reason, extra={"budget_guard": True})
    append_heartbeat("research_orchestrator", "quarantined", summary=note, task_id=task["task_id"], task_type=task["task_type"])


def _resource_exhaustion_family_key(task: dict[str, Any]) -> str | None:
    task_type = task.get("task_type")
    payload = task.get("payload") or {}
    output_dir = payload.get("output_dir") or ""
    if task_type == "generated_batch":
        batch_path = payload.get("batch_path")
        if not batch_path:
            return None
        return f"generated_batch::{batch_path}::{output_dir}"
    if task_type in {"workflow", "batch"}:
        config_path = payload.get("config_path")
        if not config_path:
            return None
        return f"{task_type}::{config_path}::{output_dir}"
    return None


def _quarantine_resource_exhausted_task_group(
    store: ExperimentStore,
    tasks: list[dict[str, Any]],
    *,
    reason: str,
    failure_count: int,
) -> list[str]:
    if not tasks:
        return []
    representative = next((task for task in tasks if task.get("status") == "pending"), tasks[0])
    moved_batch = _quarantine_batch_path(representative, reason)
    moved_output = _quarantine_output_dir(representative, reason)
    note = f"quarantined｜{reason}｜repeated_failures={failure_count}"
    if moved_batch:
        note += f"｜batch={moved_batch}"
    if moved_output:
        note += f"｜output={moved_output}"

    cleaned: list[str] = []
    for task in tasks:
        if task.get("status") not in {"pending", "running", "failed"}:
            continue
        error_text = task.get("last_error") or reason
        detail_note = note
        if error_text and error_text != reason:
            detail_note += f"｜last_error={error_text}"
        store.finish_research_task(
            task["task_id"],
            status="finished",
            last_error=error_text,
            worker_note=detail_note,
        )
        _repair_running_task_state_file(task, status="failed", error=error_text)
        cleaned.append(task["task_id"])

    opportunity_id = ((representative.get("payload") or {}).get("opportunity_id"))
    if opportunity_id:
        update_opportunity_state(
            opportunity_id,
            "rejected",
            reason=reason,
            extra={"resource_exhausted": True, "repeated_failures": failure_count},
        )
    append_heartbeat(
        "research_orchestrator",
        "quarantined",
        summary=note,
        task_id=representative["task_id"],
        task_type=representative["task_type"],
    )
    return cleaned


def _auto_quarantine_resource_exhausted_task_families(store: ExperimentStore) -> list[str]:
    thresholds = {
        "generated_batch": max(2, int(os.getenv("RESEARCH_RESOURCE_EXHAUSTION_QUARANTINE_THRESHOLD", "2"))),
        "workflow": max(1, int(os.getenv("RESEARCH_WORKFLOW_RESOURCE_EXHAUSTION_QUARANTINE_THRESHOLD", "1"))),
        "batch": max(1, int(os.getenv("RESEARCH_BATCH_RESOURCE_EXHAUSTION_QUARANTINE_THRESHOLD", "1"))),
    }
    families: dict[str, dict[str, Any]] = {}
    for task in store.list_research_tasks(limit=300):
        if task.get("task_type") not in RESOURCE_EXHAUSTION_QUARANTINE_TASK_TYPES:
            continue
        family_key = _resource_exhaustion_family_key(task)
        if not family_key:
            continue
        family = families.setdefault(family_key, {"tasks": [], "failed": [], "task_type": task.get("task_type")})
        family["tasks"].append(task)
        if task.get("status") == "failed":
            reason = _resource_exhaustion_reason(task.get("last_error") or "")
            if reason:
                family["failed"].append((task, reason))

    cleaned: list[str] = []
    for family in families.values():
        task_type = family.get("task_type") or "unknown"
        threshold = thresholds.get(task_type, 1)
        failed = family["failed"]
        if len(failed) < threshold:
            continue
        reason = failed[0][1]
        cleaned.extend(
            _quarantine_resource_exhausted_task_group(
                store,
                family["tasks"],
                reason=f"{task_type}_{reason}",
                failure_count=len(failed),
            )
        )
    return cleaned


def _auto_quarantine_budget_blocked_tasks(store: ExperimentStore, blocked_types: list[str]) -> list[str]:
    if not blocked_types:
        return []
    cleaned: list[str] = []
    for task in store.list_research_tasks(limit=300):
        if task.get("status") != "pending":
            continue
        if task.get("task_type") not in blocked_types:
            continue
        if not _task_is_budget_risky(task):
            continue
        _quarantine_budget_risky_task(store, task, reason="generated_batch_budget_guard")
        cleaned.append(task["task_id"])
    return cleaned


def run_orchestrator(max_tasks: int = 1) -> dict[str, Any]:
    store = ExperimentStore(DB_PATH)
    cleaned_running = _cleanup_stale_running_tasks(store, stale_minutes=int(os.getenv("RESEARCH_STALE_RUNNING_MINUTES", "10")))
    if cleaned_running:
        append_heartbeat(
            "research_orchestrator",
            "warning",
            summary=f"cleaned stale running tasks={len(cleaned_running)}",
        )
    cleaned_resource = _auto_quarantine_resource_exhausted_task_families(store)
    if cleaned_resource:
        append_heartbeat(
            "research_orchestrator",
            "stagnation_break",
            summary=f"auto-quarantined repeated resource-exhausted task branches={len(cleaned_resource)}",
        )
    initial_blocked_types = blocked_task_types(store)
    cleaned_budget = _auto_quarantine_budget_blocked_tasks(store, initial_blocked_types)
    if cleaned_budget:
        append_heartbeat(
            "research_orchestrator",
            "stagnation_break",
            summary=f"auto-quarantined budget-risky blocked tasks={len(cleaned_budget)}",
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
                    f"opp_injected={((planner_result.get('opportunity_execution') or {}).get('injected_count', 0))}, "
                    f"generated_outcomes={(((planner_result.get('research_metrics') or {}).get('metrics') or {}).get('generated_candidate_outcome_count', 0))}, "
                    f"research_mode={((((planner_result.get('research_metrics') or {}).get('metrics') or {}).get('research_mode') or {}).get('mode', 'unknown'))}"
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
                    forced = maybe_expand_research_space(store, max_new_tasks=4, allow_repeat=True)
                    if forced:
                        _reset_stagnation(reason="reseed_noop_forced_expand")
                        append_heartbeat("research_orchestrator", "stagnation_break", summary=f"reseed noop; forced expansion injected {len(forced)} tasks")
                    else:
                        st = _bump_stagnation(reason="reseed_attempt_noop")
                        append_heartbeat("research_orchestrator", "info", summary=f"queue empty; reseed noop; stagnation={st.get('consecutive_no_injection')}")
            else:
                forced = maybe_expand_research_space(store, max_new_tasks=4, allow_repeat=True)
                if forced:
                    _reset_stagnation(reason="cooldown_forced_expand")
                    append_heartbeat("research_orchestrator", "stagnation_break", summary=f"reseed cooldown active; forced expansion injected {len(forced)} tasks")
                else:
                    st = _bump_stagnation(reason="reseed_cooldown_active")
                    append_heartbeat("research_orchestrator", "info", summary=f"queue empty; reseed cooldown active; stagnation={st.get('consecutive_no_injection')}")

            threshold = int(os.getenv("RESEARCH_STAGNATION_THRESHOLD", "2"))
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
    blocked_types = blocked_task_types(store)
    if failure_state["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES and failure_state["cooldown_active"]:
        if blocked_types and has_pending_unblocked_tasks(store, blocked_types):
            append_heartbeat(
                "research_orchestrator",
                "circuit_open_family",
                summary=f"temporarily skipping task_types={','.join(blocked_types)} due to family failures",
            )
        else:
            append_heartbeat(
                "research_orchestrator",
                "circuit_open",
                summary=f"paused due to consecutive failures={failure_state['consecutive_failures']}",
            )
            return {
                "processed": [],
                "remaining_preview": store.list_research_tasks(limit=10),
                "guardrail": "circuit_open",
                "blocked_task_types": blocked_types,
            }

    processed = []
    append_heartbeat("research_orchestrator", "started", summary=f"orchestrator awakened (max_tasks={max_tasks})")
    claimed_tasks: list[dict[str, Any]] = []
    serial_only = False
    for _ in range(max_tasks):
        if blocked_types:
            task = store.claim_next_research_task(blocked_task_types=blocked_types)
        else:
            task = store.claim_next_research_task()
        if not task:
            break
        claimed_tasks.append(task)
        if _task_requires_serial_execution(task):
            serial_only = True
            break

    def _handle_task_result(task: dict[str, Any], summary: str) -> None:
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

    def _handle_task_exception(task: dict[str, Any], exc: Exception) -> None:
        error_text = str(exc)

        if "research task worker failed with code" in error_text and _task_outputs_look_complete(task):
            note = f"finished_with_recovered_outputs｜{error_text}"
            store.finish_research_task(task["task_id"], status="finished", worker_note=note, last_error=error_text)
            update_research_memory_from_task_result(
                "artifacts/research_memory.json",
                task,
                status="finished",
                summary=note,
                error_text=error_text,
            )
            evaluation = evaluate_opportunity_from_task(task, status="finished", summary=note)
            if evaluation:
                update_opportunity_state(evaluation["opportunity_id"], evaluation["next_state"], reason=evaluation["evaluation_label"], extra={"evaluation": evaluation, "recovered_outputs": True})
            processed.append({"task_id": task["task_id"], "status": "finished_recovered", "error": error_text, "retry_task_id": None, "opportunity_evaluation": evaluation})
            append_heartbeat(
                "research_orchestrator",
                "finished",
                summary=note,
                task_id=task["task_id"],
                task_type=task["task_type"],
            )
            return

        resource_reason = _resource_exhaustion_reason(error_text)
        if task.get("task_type") in RESOURCE_EXHAUSTION_QUARANTINE_TASK_TYPES and resource_reason:
            reason = f"{task.get('task_type')}_{resource_reason}"
            cleaned = _quarantine_resource_exhausted_task_group(
                store,
                [task],
                reason=reason,
                failure_count=max(1, int(task.get("attempt_count") or 0)),
            )
            evaluation = evaluate_opportunity_from_task(task, status="failed", error_text=error_text)
            if evaluation:
                update_opportunity_state(
                    evaluation["opportunity_id"],
                    "rejected",
                    reason=reason,
                    extra={"evaluation": evaluation, "error": error_text, "resource_exhausted": True},
                )
            processed.append(
                {
                    "task_id": task["task_id"],
                    "status": "quarantined",
                    "error": error_text,
                    "retry_task_id": None,
                    "opportunity_evaluation": evaluation,
                    "cleaned_task_ids": cleaned,
                }
            )
            return

        failure_kind = classify_task_failure(task, error_text)

        if failure_kind == "deterministic":
            reason = "deterministic_task_error"
            if error_text.startswith("Unknown field in expression:"):
                reason = "invalid_expression_field"
            elif "factor_families" in error_text and "No such file or directory" in error_text:
                reason = "missing_factor_family_config"
            elif error_text.startswith("generated config missing lineage fields:"):
                reason = "generated_config_missing_lineage"
            elif error_text.startswith("generated config references missing base factor:") or error_text.startswith("missing base factor for generated operator:"):
                reason = "generated_config_missing_base_factor"
            elif error_text.startswith("generated batch"):
                reason = "generated_batch_preflight_failed"

            quarantined_path = _quarantine_output_dir(task, reason)
            note = f"quarantined｜{reason}｜{error_text}"
            if quarantined_path:
                note += f"｜moved_to={quarantined_path}"
            store.finish_research_task(task["task_id"], status="finished", worker_note=note, last_error=error_text)
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
                    reason=reason,
                    extra={"evaluation": evaluation, "error": error_text},
                )
            processed.append({"task_id": task["task_id"], "status": "quarantined", "error": error_text, "retry_task_id": None, "opportunity_evaluation": evaluation})
            append_heartbeat(
                "research_orchestrator",
                "quarantined",
                summary=note,
                task_id=task["task_id"],
                task_type=task["task_type"],
            )
            return

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

    if claimed_tasks:
        worker_count = 1 if serial_only or any(_task_requires_serial_execution(task) for task in claimed_tasks) else max(1, min(max_tasks, len(claimed_tasks)))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_task = {executor.submit(execute_task, task): task for task in claimed_tasks}
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    _handle_task_result(task, future.result())
                except Exception as exc:
                    _handle_task_exception(task, exc)

    if not processed:
        if blocked_types and not has_pending_unblocked_tasks(store, blocked_types):
            append_heartbeat(
                "research_orchestrator",
                "idle_family_blocked",
                summary=f"only blocked task families remain pending: {','.join(blocked_types)}",
            )
        else:
            append_heartbeat("research_orchestrator", "idle", summary="no pending research tasks")
    return {
        "processed": processed,
        "remaining_preview": store.list_research_tasks(limit=10),
        "blocked_task_types": blocked_types,
    }
