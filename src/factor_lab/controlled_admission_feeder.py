from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks
from factor_lab.controlled_restart_audit import dry_run_controlled_restart

DEFAULT_OUTPUT_DIR = Path("artifacts/controlled_admission_feeder")
DEFAULT_STATE_PATH = DEFAULT_OUTPUT_DIR / "state.json"
PRODUCTION_DEFAULT_FEEDER_CONFIG: dict[str, Any] = {
    "profile": "conservative",
    "limit": 1,
    "priority": 0,
    "cooldown_minutes": 60,
    "daily_budget": 3,
    "force_new": False,
}


def load_feeder_config(path: str | Path | None = None) -> dict[str, Any]:
    cfg = dict(PRODUCTION_DEFAULT_FEEDER_CONFIG)
    if path is None:
        return cfg
    p = Path(path)
    if not p.exists():
        return cfg
    try:
        loaded = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid feeder config json: {p}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"invalid feeder config object: {p}")
    cfg.update({k: loaded[k] for k in PRODUCTION_DEFAULT_FEEDER_CONFIG if k in loaded})
    return cfg


def resolve_feeder_policy(
    config: dict[str, Any] | None = None,
    *,
    cli_overrides: dict[str, Any] | None = None,
    allow_force_new_probe: bool = False,
) -> dict[str, Any]:
    policy = dict(PRODUCTION_DEFAULT_FEEDER_CONFIG)
    if config:
        policy.update({k: config[k] for k in PRODUCTION_DEFAULT_FEEDER_CONFIG if k in config})
    if cli_overrides:
        for key, value in cli_overrides.items():
            if value is not None and key in PRODUCTION_DEFAULT_FEEDER_CONFIG:
                policy[key] = value
    policy["limit"] = int(policy["limit"])
    policy["priority"] = int(policy["priority"])
    policy["cooldown_minutes"] = int(policy["cooldown_minutes"])
    policy["daily_budget"] = int(policy["daily_budget"])
    policy["force_new"] = bool(policy["force_new"])
    if policy["force_new"] and not (allow_force_new_probe and policy.get("profile") == "probe"):
        raise ValueError("force_new is only allowed for explicit probe profile with allow_force_new_probe")
    return policy


def _now(now_utc: datetime | None = None) -> datetime:
    value = now_utc or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def should_feed_controlled_work(
    *,
    dry_run: dict[str, Any],
    recent_injection_count: int,
    daily_injection_count: int,
    cooldown_ready: bool,
    daily_budget: int,
    limit: int = 1,
) -> dict[str, Any]:
    reasons: list[str] = []
    if int(dry_run.get("would_run_count") or 0) > 0:
        reasons.append("already_has_claimable_workflow")
    if not cooldown_ready:
        reasons.append("cooldown_not_ready")
    if int(daily_injection_count) >= int(daily_budget):
        reasons.append("daily_budget_exhausted")
    if reasons:
        return {"decision": "skip", "reasons": reasons, "limit": 0}
    return {"decision": "feed", "reasons": [], "limit": max(1, int(limit))}


def load_feeder_state(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"events": []}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"events": []}
    if not isinstance(payload, dict):
        return {"events": []}
    events = payload.get("events")
    if not isinstance(events, list):
        payload["events"] = []
    return payload


def feeder_state_counts(*, state: dict[str, Any], now_utc: datetime, cooldown_minutes: int) -> dict[str, int]:
    now = _now(now_utc)
    today = now.date()
    cooldown_since = now - timedelta(minutes=max(0, int(cooldown_minutes)))
    daily = 0
    recent = 0
    for event in state.get("events") or []:
        try:
            ts = _parse_time(str(event.get("created_at_utc")))
        except (TypeError, ValueError):
            continue
        count = int(event.get("enqueued_count") or 0)
        if ts.date() == today:
            daily += count
        if ts >= cooldown_since:
            recent += count
    return {"daily_injection_count": daily, "recent_injection_count": recent}


def feeder_route_counts(state: dict[str, Any], *, now_utc: datetime, lookback_hours: int = 24) -> dict[str, int]:
    now = _now(now_utc)
    since = now - timedelta(hours=max(0, int(lookback_hours)))
    counts: dict[str, int] = {}
    for event in state.get("events") or []:
        try:
            ts = _parse_time(str(event.get("created_at_utc")))
        except (TypeError, ValueError):
            continue
        if ts < since:
            continue
        routes = event.get("routes") or []
        if isinstance(routes, str):
            routes = [routes]
        for route in routes:
            if route:
                counts[str(route)] = counts.get(str(route), 0) + 1
    return counts


def record_feeder_event(path: str | Path, event: dict[str, Any]) -> None:
    p = Path(path)
    state = load_feeder_state(p)
    state.setdefault("events", []).append(event)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_outputs(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "feeder_run.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Controlled Admission Feeder Run",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Write: {payload.get('write')}",
        f"Profile: {payload.get('profile')}",
        f"Decision: {(payload.get('decision') or {}).get('decision')}",
        f"Reasons: {(payload.get('decision') or {}).get('reasons')}",
        f"Runtime interpretation: {payload.get('runtime_interpretation')}",
        f"Enqueued: {((payload.get('prepare_result') or {}).get('enqueued_count') if payload.get('prepare_result') else 0)}",
        "",
    ]
    (output_dir / "feeder_run.md").write_text("\n".join(lines), encoding="utf-8")


def interpret_feeder_runtime(*, dry_run: dict[str, Any], decision: dict[str, Any], prepare_result: dict[str, Any] | None) -> str:
    if decision.get("decision") == "feed":
        if int((prepare_result or {}).get("enqueued_count") or 0) > 0:
            return "fed_one_workflow"
        return "idle_no_claimable_workflow"
    reasons = decision.get("reasons") or []
    if "already_has_claimable_workflow" in reasons:
        return "skipped_existing_work"
    if "cooldown_not_ready" in reasons:
        return "skipped_cooldown"
    if "daily_budget_exhausted" in reasons:
        return "skipped_budget"
    if int(dry_run.get("would_run_count") or 0) <= 0:
        return "idle_no_claimable_workflow"
    return "skipped"


def run_controlled_admission_feeder(
    *,
    db_path: str | Path = "artifacts/factor_lab.db",
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    state_path: str | Path | None = None,
    write: bool = False,
    limit: int = 1,
    priority: int = 0,
    cooldown_minutes: int = 60,
    daily_budget: int = 3,
    force_new: bool = False,
    profile: str = "conservative",
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now = _now(now_utc)
    out = Path(output_dir)
    state_file = Path(state_path) if state_path is not None else out / "state.json"
    dry_run = dry_run_controlled_restart(db_path=db_path)
    state = load_feeder_state(state_file)
    counts = feeder_state_counts(state=state, now_utc=now, cooldown_minutes=cooldown_minutes)
    route_counts = feeder_route_counts(state, now_utc=now, lookback_hours=24)
    decision = should_feed_controlled_work(
        dry_run=dry_run,
        recent_injection_count=counts["recent_injection_count"],
        daily_injection_count=counts["daily_injection_count"],
        cooldown_ready=counts["recent_injection_count"] <= 0,
        daily_budget=daily_budget,
        limit=limit,
    )
    prepare_result: dict[str, Any] | None = None
    if decision["decision"] == "feed":
        prepare_result = prepare_bucket_aware_tasks(
            db_path=db_path,
            dry_run=not write,
            limit=decision["limit"],
            priority=priority,
            force_new=force_new,
            route_history_counts=route_counts,
        )
        if write and int(prepare_result.get("enqueued_count") or 0) > 0:
            record_feeder_event(
                state_file,
                {
                    "created_at_utc": now.isoformat(),
                    "enqueued_count": int(prepare_result.get("enqueued_count") or 0),
                    "task_ids": prepare_result.get("task_ids") or [],
                    "routes": [str((task.get("payload") or {}).get("route_id")) for task in (prepare_result.get("tasks") or []) if (task.get("payload") or {}).get("route_id")],
                    "decision": decision,
                },
            )
    runtime_interpretation = interpret_feeder_runtime(dry_run=dry_run, decision=decision, prepare_result=prepare_result)
    payload = {
        "generated_at_utc": now.isoformat(),
        "write": bool(write),
        "profile": profile,
        "effective_policy": {
            "limit": int(limit),
            "priority": int(priority),
            "cooldown_minutes": int(cooldown_minutes),
            "daily_budget": int(daily_budget),
            "force_new": bool(force_new),
        },
        "runtime_interpretation": runtime_interpretation,
        "safety": {"broad_daemon_allowed": False, "controlled_only_allowed": True},
        "db_path": str(db_path),
        "state_path": str(state_file),
        "dry_run": dry_run,
        "state_counts": counts,
        "route_history_counts": route_counts,
        "cooldown_minutes": int(cooldown_minutes),
        "daily_budget": int(daily_budget),
        "decision": decision,
        "prepare_result": prepare_result,
    }
    _write_outputs(out, payload)
    return payload
