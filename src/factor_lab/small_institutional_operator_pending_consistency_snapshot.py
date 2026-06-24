from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_flags(consistency: dict[str, Any], weekly: dict[str, Any], canonical: dict[str, Any]) -> dict[str, bool]:
    return {
        "queue_write_allowed": bool(
            consistency.get("queue_write_allowed")
            or weekly.get("queue_write_allowed")
            or canonical.get("queue_write_allowed")
        ),
        "broad_daemon_allowed": bool(
            consistency.get("broad_daemon_allowed")
            or weekly.get("broad_daemon_allowed")
            or canonical.get("broad_daemon_allowed")
        ),
        "automation_allowed": bool(
            consistency.get("automation_allowed")
            or weekly.get("automation_allowed")
            or canonical.get("automation_allowed")
        ),
        "automated_rerun_allowed": bool(
            consistency.get("automated_rerun_allowed")
            or weekly.get("automated_rerun_allowed")
            or canonical.get("automated_rerun_allowed")
        ),
        "live_trading_enabled": bool(
            consistency.get("live_trading_enabled")
            or weekly.get("live_trading_enabled")
            or canonical.get("live_trading_enabled")
        ),
    }


def _observation_summary(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    return {
        "observation_status": payload.get("observation_status"),
        "primary_issue": payload.get("primary_issue"),
        "manual_approval_status": payload.get("manual_approval_status"),
        "benchmark_id": payload.get("benchmark_id"),
        "turnover_one_way_estimate": payload.get("turnover_one_way_estimate"),
        "estimated_round_trip_cost": payload.get("estimated_round_trip_cost"),
        "queue_write_allowed": bool(payload.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(payload.get("broad_daemon_allowed")),
        "automation_allowed": bool(payload.get("automation_allowed")),
        "automated_rerun_allowed": bool(payload.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(payload.get("live_trading_enabled")),
    }


def build_operator_pending_consistency_snapshot(
    *,
    status_path: str | Path,
    generated_at: str | None = None,
) -> dict[str, Any]:
    status = _load_json(status_path)
    paper_monitoring = status.get("paper_monitoring") or {}
    consistency = paper_monitoring.get("operator_pending_consistency") or {}
    weekly = _observation_summary(paper_monitoring.get("operator_pending_observation") or {})
    canonical = _observation_summary(status.get("operator_pending_observation") or {})

    snapshot_status = "ready" if consistency else "missing_consistency"
    runtime = _runtime_flags(consistency, weekly, canonical)
    context_source = weekly or canonical
    return {
        "schema_version": 1,
        "generated_at_utc": generated_at or datetime.now(timezone.utc).isoformat(),
        "snapshot_status": snapshot_status,
        "source_status_generated_at_utc": status.get("generated_at_utc"),
        "consistency_status": consistency.get("consistency_status") if consistency else None,
        "mismatches": list(consistency.get("mismatches") or []),
        "weekly_operator_pending": weekly,
        "canonical_operator_pending": canonical,
        "benchmark_id": context_source.get("benchmark_id"),
        "turnover_one_way_estimate": context_source.get("turnover_one_way_estimate"),
        "estimated_round_trip_cost": context_source.get("estimated_round_trip_cost"),
        "runtime": runtime,
        "next_action": "await_operator_decision_no_automation",
    }


def operator_pending_consistency_snapshot_to_markdown(payload: dict[str, Any]) -> str:
    runtime = payload.get("runtime") or {}
    mismatches = payload.get("mismatches") or []
    weekly = payload.get("weekly_operator_pending") or {}
    canonical = payload.get("canonical_operator_pending") or {}
    lines = [
        "# Operator-Pending Consistency Snapshot",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Snapshot status: {payload.get('snapshot_status')}",
        f"Source status generated_at_utc: {payload.get('source_status_generated_at_utc')}",
        "",
        "## Consistency",
        f"- Consistency status: {payload.get('consistency_status')}",
        "- Mismatches:",
    ]
    if mismatches:
        lines.extend(f"  - {item}" for item in mismatches)
    else:
        lines.append("  - None")
    lines.extend(
        [
            "",
            "## Observation summaries",
            f"- Weekly observation_status: {weekly.get('observation_status')}",
            f"- Canonical observation_status: {canonical.get('observation_status')}",
            f"- benchmark_id: {payload.get('benchmark_id')}",
            f"- turnover_one_way_estimate: {payload.get('turnover_one_way_estimate')}",
            f"- estimated_round_trip_cost: {payload.get('estimated_round_trip_cost')}",
            "",
            "## Runtime safety",
            f"- Queue write allowed: {runtime.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {runtime.get('broad_daemon_allowed')}",
            f"- Automation allowed: {runtime.get('automation_allowed')}",
            f"- Automated rerun allowed: {runtime.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {runtime.get('live_trading_enabled')}",
            "",
            f"Next action: {payload.get('next_action')}",
        ]
    )
    return "\n".join(lines) + "\n"


def write_operator_pending_consistency_snapshot(
    *,
    status_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
    generated_at: str | None = None,
) -> dict[str, Any]:
    payload = build_operator_pending_consistency_snapshot(
        status_path=status_path,
        generated_at=generated_at,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(operator_pending_consistency_snapshot_to_markdown(payload), encoding="utf-8")
    return payload


def write_status_then_operator_pending_consistency_snapshot(
    *,
    status_json_path: str | Path,
    status_markdown_path: str | Path,
    status_knowledge_path: str | Path,
    snapshot_json_path: str | Path,
    snapshot_markdown_path: str | Path,
    status_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Refresh status, then snapshot, then status again so the final embedded snapshot is fresh.

    This helper is intentionally metadata-only: it only rewrites local status/snapshot
    artifacts and delegates to the existing status/snapshot writers. It does not touch
    workflow queues, research queues, daemons, risk limits, or live-trading flags.
    """
    from factor_lab.small_institutionalization_policy import write_small_institutionalization_status

    kwargs = dict(status_kwargs or {})
    kwargs.setdefault("operator_pending_consistency_snapshot_path", snapshot_json_path)
    first_status = write_small_institutionalization_status(
        json_path=status_json_path,
        markdown_path=status_markdown_path,
        knowledge_path=status_knowledge_path,
        **kwargs,
    )
    snapshot = write_operator_pending_consistency_snapshot(
        status_path=status_json_path,
        json_path=snapshot_json_path,
        markdown_path=snapshot_markdown_path,
    )
    final_kwargs = dict(kwargs)
    if first_status.get("generated_at_utc"):
        final_kwargs["generated_at"] = first_status["generated_at_utc"]
    final_status = write_small_institutionalization_status(
        json_path=status_json_path,
        markdown_path=status_markdown_path,
        knowledge_path=status_knowledge_path,
        **final_kwargs,
    )
    return {"first_status": first_status, "snapshot": snapshot, "status": final_status}
