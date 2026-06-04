from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

HARVEST_ROOT = Path("artifacts/harvest_agent")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _cycle_sort_key(path: Path) -> tuple[int, str]:
    try:
        return (int(path.name.split("_")[-1]), path.name)
    except Exception:
        return (-1, path.name)


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _best_sharpe(analysis: dict[str, Any], oos: dict[str, Any]) -> float | None:
    value = _as_float(oos.get("best_sharpe"))
    if value is not None:
        return value
    value = _as_float(analysis.get("best_sharpe"))
    if value is not None:
        return value
    best = analysis.get("best_result") or {}
    return _as_float(best.get("sharpe") or best.get("sharpe_net") or best.get("best_sharpe"))


def _max_drawdown(analysis: dict[str, Any], oos: dict[str, Any]) -> float | None:
    for value in [oos.get("max_drawdown"), oos.get("worst_drawdown"), analysis.get("max_drawdown"), analysis.get("worst_drawdown")]:
        parsed = _as_float(value)
        if parsed is not None:
            return parsed
    best = analysis.get("best_result") or {}
    return _as_float(best.get("max_drawdown") or best.get("worst_drawdown"))


def _extract_blockers(failure: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    for key in ["primary_blockers", "blockers", "reason_codes"]:
        value = failure.get(key)
        if isinstance(value, list):
            blockers.extend(str(v) for v in value if v)
        elif isinstance(value, str) and value:
            blockers.append(value)
    groups = failure.get("groups") or {}
    if isinstance(groups, dict):
        for group in groups.values():
            if isinstance(group, dict):
                reasons = group.get("blockers") or group.get("reasons") or []
                if isinstance(reasons, list):
                    blockers.extend(str(v) for v in reasons if v)
    return blockers


def _load_cycle(path: Path) -> dict[str, Any]:
    analysis = _read_json(path / "result_analysis.json")
    oos = _read_json(path / "oos_validation.json")
    failure = _read_json(path / "failure_attribution.json")
    route_state = _read_json(path / "route_state.json")
    decision = _read_json(path / "research_decision.json")
    semantic = _read_json(path / "semantic_signature.json")
    mechanism = _read_json(path / "mechanism_route.json")
    next_plan = _read_json(path / "v3_next_cycle_plan.json")
    branch = decision.get("decision") or next_plan.get("branch")
    mechanism_id = decision.get("mechanism_id") or mechanism.get("mechanism_id") or route_state.get("current_route")
    return {
        "cycle_id": path.name,
        "branch": branch,
        "oos_class": oos.get("oos_class") or next_plan.get("oos_class"),
        "best_sharpe": _best_sharpe(analysis, oos),
        "max_drawdown": _max_drawdown(analysis, oos),
        "failure_blockers": _extract_blockers(failure),
        "semantic_hash": semantic.get("semantic_hash"),
        "mechanism_id": mechanism_id,
        "current_route_status": route_state.get("current_route_status") or route_state.get("route_status"),
        "manual_approval_required": bool(decision.get("manual_approval_required") or next_plan.get("manual_approval_required")),
        "data_request": decision.get("data_request") or {},
        "source_dir": str(path),
    }


def _load_controller_runs(base: Path) -> tuple[list[dict[str, Any]], str | None]:
    controller_base = base / "controller_runs"
    latest_pointer = _read_json(base / "latest_controller_run.json")
    latest_id = latest_pointer.get("controller_run_id")
    runs: list[dict[str, Any]] = []
    if controller_base.exists():
        for run_dir in sorted([p for p in controller_base.iterdir() if p.is_dir()], key=lambda p: p.name):
            summary = _read_json(run_dir / "controller_summary.json")
            if not summary:
                summary = {"controller_run_id": run_dir.name}
            summary.setdefault("controller_run_id", run_dir.name)
            summary["source_dir"] = str(run_dir)
            runs.append(summary)
    if not latest_id and runs:
        latest_id = str(runs[-1].get("controller_run_id") or runs[-1].get("source_dir"))
    return runs, str(latest_id) if latest_id else None


def collect_strategy_evidence(root: str | Path = ".", lookback_cycles: int = 8) -> dict[str, Any]:
    root = Path(root)
    base = root / HARVEST_ROOT
    cycle_dirs = sorted([p for p in base.glob("cycle_*") if p.is_dir()], key=_cycle_sort_key)
    selected = cycle_dirs[-max(0, int(lookback_cycles)):] if lookback_cycles else []
    cycles = [_load_cycle(path) for path in selected]
    controllers, latest_controller_id = _load_controller_runs(base)
    branch_sequence = [str(c.get("branch")) for c in cycles if c.get("branch")]
    blockers = Counter(b for c in cycles for b in c.get("failure_blockers", []) if b)
    semantic_counts = Counter(str(c.get("semantic_hash")) for c in cycles if c.get("semantic_hash"))
    route_counts = Counter(str(c.get("mechanism_id")) for c in cycles if c.get("mechanism_id"))
    current_route_status = next((c.get("current_route_status") for c in reversed(cycles) if c.get("current_route_status")), None)
    missing_fields: list[str] = []
    for c in cycles:
        data_request = c.get("data_request") or {}
        fields = data_request.get("missing_fields") if isinstance(data_request, dict) else None
        if isinstance(fields, list):
            missing_fields.extend(str(f) for f in fields if f)
    evidence = {
        "schema_version": 1,
        "latest_cycle_id": cycles[-1]["cycle_id"] if cycles else None,
        "latest_controller_run_id": latest_controller_id,
        "cycles": cycles,
        "controller_runs": controllers,
        "branch_sequence": branch_sequence,
        "failure_blocker_counts": dict(blockers),
        "semantic_hash_counts": dict(semantic_counts),
        "route_counts": dict(route_counts),
        "current_route_status": current_route_status,
        "missing_required_fields": sorted(set(missing_fields)),
    }
    evidence["loop_analysis"] = detect_strategy_loops(evidence)
    return evidence


def _is_alternating_loop(branches: list[str]) -> bool:
    if len(branches) < 4:
        return False
    tail = branches[-4:]
    return tail[0] == tail[2] and tail[1] == tail[3] and tail[0] != tail[1]


def detect_strategy_loops(evidence: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    blocked_branches: list[str] = []
    branches = [str(b) for b in evidence.get("branch_sequence") or [] if b]
    if _is_alternating_loop(branches):
        reason_codes.append("branch_loop_detected")
        blocked_branches.extend(branches[-4:])

    cycles = list(evidence.get("cycles") or [])
    failed = [c for c in cycles if c.get("oos_class") == "fail"]
    if len(failed) >= 2:
        reason_codes.append("repeated_oos_failures")
        prev, latest = failed[-2], failed[-1]
        prev_sharpe = _as_float(prev.get("best_sharpe"))
        latest_sharpe = _as_float(latest.get("best_sharpe"))
        if prev_sharpe is not None and latest_sharpe is not None and latest_sharpe <= prev_sharpe:
            reason_codes.append("sharpe_not_improving")
        prev_dd = _as_float(prev.get("max_drawdown"))
        latest_dd = _as_float(latest.get("max_drawdown"))
        if prev_dd is not None and latest_dd is not None and latest_dd <= prev_dd:
            reason_codes.append("drawdown_not_improving")

    semantic_counts = evidence.get("semantic_hash_counts") or {}
    repeated_hashes = [h for h, count in semantic_counts.items() if int(count) >= 2]
    if repeated_hashes:
        reason_codes.append("semantic_repeat_limit_reached")

    seen: set[str] = set()
    unique_reasons = [r for r in reason_codes if not (r in seen or seen.add(r))]
    blocked_unique = sorted(set(blocked_branches))
    return {
        "schema_version": 1,
        "loop_detected": bool(unique_reasons),
        "reason_codes": unique_reasons,
        "blocked_branches": blocked_unique,
        "semantic_repeats": sorted(repeated_hashes),
    }
