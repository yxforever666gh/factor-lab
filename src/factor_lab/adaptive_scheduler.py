from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.user_idle_detector import user_idle_snapshot

ROOT = Path(__file__).resolve().parents[2]
SCHEDULER_STATE_PATH = ROOT / "artifacts" / "adaptive_scheduler_state.json"
TUSHARE_ROUTE_STATUS_PATH = ROOT / "artifacts" / "tushare_route_status.json"


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        return float(raw) if raw else float(default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(float(raw)) if raw else int(default)
    except Exception:
        value = int(default)
    return max(minimum, value)


def _base_env_int(name: str, default: int, *, minimum: int = 0) -> int:
    base_key = f"FACTOR_LAB_BASE_{name}"
    if not (os.getenv(base_key) or "").strip():
        current = (os.getenv(name) or "").strip()
        os.environ[base_key] = current if current else str(default)
    return _env_int(base_key, default, minimum=minimum)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def route_status_snapshot() -> dict[str, Any]:
    payload = _read_json(TUSHARE_ROUTE_STATUS_PATH)
    if not payload:
        return {"healthy": True, "resolved_mode": None}
    return {
        "healthy": bool(payload.get("healthy", True)),
        "resolved_mode": payload.get("resolved_mode"),
        "last_error": payload.get("last_error"),
        "last_probe_ms": payload.get("last_probe_ms"),
    }


def _cpu_targets(user_mode: str) -> tuple[float, float]:
    if user_mode == "interactive":
        return (
            _env_float("FACTOR_LAB_CPU_TARGET_INTERACTIVE_MIN", 0.35),
            _env_float("FACTOR_LAB_CPU_TARGET_INTERACTIVE_MAX", 0.55),
        )
    if user_mode == "background_idle":
        return (
            _env_float("FACTOR_LAB_CPU_TARGET_IDLE_MIN", 0.70),
            _env_float("FACTOR_LAB_CPU_TARGET_IDLE_MAX", 0.85),
        )
    return (
        _env_float("FACTOR_LAB_CPU_TARGET_UNKNOWN_MIN", 0.45),
        _env_float("FACTOR_LAB_CPU_TARGET_UNKNOWN_MAX", 0.70),
    )


def detect_scheduler_mode(
    *,
    cpu_usage_ratio: float,
    mem_pressure: float,
    mem_available_mb: int,
    route_healthy: bool,
    idle_snapshot: dict[str, Any] | None = None,
) -> str:
    idle_snapshot = idle_snapshot or user_idle_snapshot()
    user_mode = idle_snapshot.get("mode") or "unknown"
    if mem_available_mb and mem_available_mb < 2048:
        return "resource_guard"
    if mem_pressure >= 0.88:
        return "resource_guard"
    if cpu_usage_ratio >= 1.15 and user_mode == "interactive":
        return "resource_guard"
    if not route_healthy and cpu_usage_ratio < 0.55:
        return "io_blocked"
    if user_mode == "interactive":
        return "interactive"
    if user_mode == "background_idle":
        return "background_idle"
    return "unknown"


def compute_scheduler_policy(
    *,
    base_max_tasks: int,
    cpu_usage_ratio: float,
    mem_pressure: float,
    mem_available_mb: int,
    rss_ratio: float,
    idle_snapshot: dict[str, Any] | None = None,
    route_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    idle_snapshot = idle_snapshot or user_idle_snapshot()
    route_status = route_status or route_status_snapshot()
    route_healthy = bool(route_status.get("healthy", True))
    mode = detect_scheduler_mode(
        cpu_usage_ratio=cpu_usage_ratio,
        mem_pressure=mem_pressure,
        mem_available_mb=mem_available_mb,
        route_healthy=route_healthy,
        idle_snapshot=idle_snapshot,
    )
    target_min, target_max = _cpu_targets(idle_snapshot.get("mode") or "unknown")

    cpu_budget_max = _env_int("FACTOR_LAB_CPU_BUDGET_MAX", 5, minimum=1)
    network_budget_max = _env_int("FACTOR_LAB_NETWORK_BUDGET_MAX", 2, minimum=0)
    light_budget_max = _env_int("FACTOR_LAB_LIGHT_TASK_BUDGET_MAX", 2, minimum=1)
    step_up = _env_int("FACTOR_LAB_SCHEDULER_MAX_STEP_UP", 1, minimum=1)
    step_down = _env_int("FACTOR_LAB_SCHEDULER_MAX_STEP_DOWN", 2, minimum=1)

    dynamic_max_tasks = max(1, min(base_max_tasks, cpu_budget_max))
    dynamic_batch_workers = 1
    queue_validation_cap = max(1, _base_env_int("RESEARCH_QUEUE_MAX_PENDING_VALIDATION", 3, minimum=1))
    queue_exploration_cap = max(1, _base_env_int("RESEARCH_QUEUE_MAX_PENDING_EXPLORATION", 3, minimum=1))
    queue_baseline_cap = max(1, _base_env_int("RESEARCH_QUEUE_MAX_PENDING_BASELINE", 2, minimum=1))
    opportunity_enqueue_limit = max(1, _base_env_int("RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT", 8, minimum=1))
    cpu_budget = min(cpu_budget_max, max(1, base_max_tasks))
    network_budget = min(network_budget_max, queue_exploration_cap)
    light_task_budget = light_budget_max
    reason = mode

    if mode == "interactive":
        cpu_budget = max(1, min(base_max_tasks, 2))
        network_budget = min(network_budget, 1)
        dynamic_max_tasks = cpu_budget
        dynamic_batch_workers = 1
        # Keep execution conservative while preserving enough pending backlog so the queue
        # does not collapse to zero between loops.
        queue_exploration_cap = max(2, min(queue_exploration_cap, 2))
        queue_validation_cap = max(2, min(queue_validation_cap, 3))
        queue_baseline_cap = 1
        opportunity_enqueue_limit = max(2, min(opportunity_enqueue_limit, 3))
        reason = "interactive_session_active"
    elif mode == "background_idle":
        cpu_budget = min(cpu_budget_max, max(base_max_tasks + step_up, 4))
        dynamic_max_tasks = cpu_budget
        dynamic_batch_workers = 3 if route_healthy and mem_pressure < 0.78 and rss_ratio < 0.8 else 2
        network_budget = min(network_budget_max, 3 if route_healthy else 1)
        queue_exploration_cap = max(queue_exploration_cap, network_budget)
        queue_validation_cap = max(queue_validation_cap, 3)
        queue_baseline_cap = max(queue_baseline_cap, 2)
        opportunity_enqueue_limit = max(opportunity_enqueue_limit, 8)
        reason = "background_idle_expand"
    elif mode == "io_blocked":
        cpu_budget = max(1, min(base_max_tasks + step_up, cpu_budget_max))
        dynamic_max_tasks = cpu_budget
        dynamic_batch_workers = 1
        network_budget = 0
        queue_exploration_cap = 1
        queue_validation_cap = max(2, min(queue_validation_cap, 3))
        queue_baseline_cap = max(1, min(queue_baseline_cap, 2))
        opportunity_enqueue_limit = min(opportunity_enqueue_limit, 2)
        reason = "route_unhealthy_prefer_local_work"
    elif mode == "resource_guard":
        cpu_budget = 1
        dynamic_max_tasks = 1
        dynamic_batch_workers = 1
        network_budget = 0
        queue_exploration_cap = 1
        queue_validation_cap = max(1, min(queue_validation_cap, 2))
        queue_baseline_cap = 1
        opportunity_enqueue_limit = 1
        reason = "resource_guard"
    else:
        if cpu_usage_ratio < target_min and mem_pressure < 0.78:
            dynamic_max_tasks = min(cpu_budget_max, max(base_max_tasks + step_up, base_max_tasks))
            dynamic_batch_workers = 2 if route_healthy else 1
            network_budget = min(network_budget_max, 2 if route_healthy else 1)
            reason = "cpu_below_target"
        elif cpu_usage_ratio > target_max or mem_pressure >= 0.84:
            dynamic_max_tasks = max(1, base_max_tasks - step_down)
            dynamic_batch_workers = 1
            network_budget = min(network_budget, 1)
            queue_exploration_cap = min(queue_exploration_cap, 2)
            opportunity_enqueue_limit = min(opportunity_enqueue_limit, 4)
            reason = "cpu_above_target"
        else:
            dynamic_max_tasks = max(1, min(base_max_tasks, cpu_budget_max))
            dynamic_batch_workers = 2 if route_healthy else 1
            reason = "target_zone"

    policy = {
        "mode": mode,
        "reason": reason,
        "cpu_target_min": round(target_min, 3),
        "cpu_target_max": round(target_max, 3),
        "cpu_budget": int(cpu_budget),
        "network_budget": int(max(0, network_budget)),
        "light_task_budget": int(max(1, light_task_budget)),
        "dynamic_max_tasks": int(max(1, min(cpu_budget_max, dynamic_max_tasks))),
        "dynamic_batch_workers": int(max(1, min(4, dynamic_batch_workers))),
        "queue_caps": {
            "baseline": int(max(1, queue_baseline_cap)),
            "validation": int(max(1, queue_validation_cap)),
            "exploration": int(max(1, queue_exploration_cap)),
        },
        "opportunity_enqueue_limit": int(max(1, opportunity_enqueue_limit)),
        "route": route_status,
        "idle": idle_snapshot,
    }
    return policy


def apply_scheduler_env(policy: dict[str, Any]) -> None:
    queue_caps = policy.get("queue_caps") or {}
    os.environ["FACTOR_LAB_BATCH_MAX_WORKERS"] = str(int(policy.get("dynamic_batch_workers") or 1))
    os.environ["RESEARCH_QUEUE_MAX_PENDING_BASELINE"] = str(int(queue_caps.get("baseline") or 1))
    os.environ["RESEARCH_QUEUE_MAX_PENDING_VALIDATION"] = str(int(queue_caps.get("validation") or 1))
    os.environ["RESEARCH_QUEUE_MAX_PENDING_EXPLORATION"] = str(int(queue_caps.get("exploration") or 1))
    os.environ["RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT"] = str(int(policy.get("opportunity_enqueue_limit") or 1))


def write_scheduler_state(policy: dict[str, Any], *, cpu_usage_ratio: float, mem_pressure: float, rss_ratio: float) -> None:
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": policy.get("mode"),
        "reason": policy.get("reason"),
        "cpu_target_min": policy.get("cpu_target_min"),
        "cpu_target_max": policy.get("cpu_target_max"),
        "cpu_budget": policy.get("cpu_budget"),
        "network_budget": policy.get("network_budget"),
        "light_task_budget": policy.get("light_task_budget"),
        "dynamic_max_tasks": policy.get("dynamic_max_tasks"),
        "dynamic_batch_workers": policy.get("dynamic_batch_workers"),
        "queue_caps": policy.get("queue_caps"),
        "opportunity_enqueue_limit": policy.get("opportunity_enqueue_limit"),
        "route": policy.get("route") or {},
        "idle": policy.get("idle") or {},
        "cpu_usage_ratio": round(cpu_usage_ratio, 4),
        "mem_pressure": round(mem_pressure, 4),
        "rss_ratio": round(rss_ratio, 4),
    }
    _write_json(SCHEDULER_STATE_PATH, payload)
