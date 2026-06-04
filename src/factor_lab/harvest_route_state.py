from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _oos_class(oos: dict[str, Any]) -> str:
    return str(oos.get("oos_class") or oos.get("class") or "unknown")


def _cycle_dirs(root: Path) -> list[Path]:
    base = root / "artifacts" / "harvest_agent"
    return sorted([p for p in base.glob("cycle_*") if p.is_dir()]) if base.exists() else []


def _route_status(summary: dict[str, Any], policy: dict[str, Any]) -> str:
    if summary.get("near_miss_count", 0) > 0 and policy.get("near_miss_keeps_route_active", True):
        return "active"
    consecutive = int(summary.get("consecutive_failures") or 0)
    max_repeat = max((summary.get("semantic_repeats") or {"": 0}).values() or [0])
    if consecutive >= policy["max_consecutive_failures_before_demote"] and max_repeat >= policy["max_semantic_repeats_before_stop"]:
        return "stop"
    if consecutive >= policy["max_consecutive_failures_before_demote"]:
        return "demote"
    if consecutive >= policy["max_consecutive_failures_before_hold"]:
        return "hold"
    if summary.get("fail_count", 0):
        return "watch"
    return "active"


def build_route_state(root: str | Path = ".", current_route: str | None = None) -> dict[str, Any]:
    root = Path(root)
    policy = {
        "max_consecutive_failures_before_hold": 2,
        "max_consecutive_failures_before_demote": 3,
        "max_semantic_repeats_before_stop": 3,
        "near_miss_keeps_route_active": True,
    }
    route_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cdir in _cycle_dirs(root):
        route = _load(cdir / "mechanism_route.json")
        if not route.get("mechanism_id"):
            continue
        oos = _load(cdir / "oos_validation.json")
        diagnosis = _load(cdir / "diagnosis.json")
        sem = _load(cdir / "semantic_signature.json")
        rid = str(route.get("mechanism_id") or "unknown")
        route_events[rid].append({
            "cycle_id": cdir.name,
            "oos_class": _oos_class(oos),
            "failure_classes": list(diagnosis.get("failure_classes") or oos.get("reasons") or []),
            "semantic_hash": sem.get("semantic_hash") or sem.get("hash"),
            "best_sharpe": oos.get("best_sharpe"),
            "worst_drawdown": oos.get("worst_drawdown"),
        })

    routes: dict[str, Any] = {}
    for rid, events in route_events.items():
        classes = [_oos_class(e) for e in events]
        consecutive = 0
        for cls in reversed(classes):
            if cls == "fail":
                consecutive += 1
            else:
                break
        repeated_failures = Counter(fc for e in events for fc in e.get("failure_classes") or [])
        sem_counts = Counter(str(e.get("semantic_hash")) for e in events if e.get("semantic_hash"))
        summary = {
            "attempts": len(events),
            "pass_count": classes.count("pass"),
            "near_miss_count": classes.count("near_miss"),
            "fail_count": classes.count("fail"),
            "consecutive_failures": consecutive,
            "repeated_failure_classes": dict(repeated_failures),
            "semantic_repeats": dict(sem_counts),
            "latest_best_sharpe": events[-1].get("best_sharpe"),
            "latest_worst_drawdown": events[-1].get("worst_drawdown"),
            "events": events,
        }
        summary["status"] = _route_status(summary, policy)
        routes[rid] = summary

    current_status = routes.get(current_route or "", {}).get("status", "active")
    return {"schema_version": 1, "policy": policy, "current_route": current_route, "current_route_status": current_status, "routes": routes}
