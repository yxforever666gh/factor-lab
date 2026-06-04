from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_FORBIDDEN_ACTIONS = [
    "no_broad_daemon_restore",
    "no_queue_write",
    "no_timer_enable",
    "no_auto_promotion",
    "no_live_trading",
    "no_model_provider_pinning",
    "no_drawdown_limit_relaxation",
]

_VALID_ROLES = {"diagnostician", "implementer", "verifier", "reviewer", "knowledge_steward", "stop"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_hermes_correction_state(
    *,
    correction_id: str,
    failure_target: str,
    created_at_utc: str | None = None,
    source_artifacts: list[dict[str, Any]] | None = None,
    diagnosis: dict[str, Any] | None = None,
    allowed_actions: list[str] | None = None,
    forbidden_actions: list[str] | None = None,
    next_agent_role: str = "diagnostician",
    manual_review_required: bool = True,
    queue_write_allowed: bool = False,
    automation_allowed: bool = False,
    live_trading_enabled: bool = False,
) -> dict[str, Any]:
    """Build a conservative Hermes correction handoff state.

    Safety flags are intentionally monotonic-conservative: callers cannot relax
    manual review, queue write, automation, or live-trading guardrails through
    this builder.
    """
    role = next_agent_role if next_agent_role in _VALID_ROLES else "diagnostician"
    merged_forbidden = list(dict.fromkeys([*(forbidden_actions or []), *DEFAULT_FORBIDDEN_ACTIONS]))
    return {
        "schema_version": 1,
        "correction_id": str(correction_id),
        "created_at_utc": created_at_utc or _utc_now(),
        "failure_target": str(failure_target),
        "source_artifacts": list(source_artifacts or []),
        "diagnosis": dict(diagnosis or {}),
        "allowed_actions": list(allowed_actions or []),
        "forbidden_actions": merged_forbidden,
        "next_agent_role": role,
        "manual_review_required": True,
        "queue_write_allowed": False,
        "automation_allowed": False,
        "live_trading_enabled": False,
    }


def hermes_correction_state_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Hermes Correction State",
        "",
        f"schema_version: {payload.get('schema_version')}",
        f"correction_id: {payload.get('correction_id')}",
        f"created_at_utc: {payload.get('created_at_utc')}",
        f"failure_target: {payload.get('failure_target')}",
        f"next_agent_role: {payload.get('next_agent_role')}",
        f"manual_review_required: {payload.get('manual_review_required')}",
        f"queue_write_allowed: {payload.get('queue_write_allowed')}",
        f"automation_allowed: {payload.get('automation_allowed')}",
        f"live_trading_enabled: {payload.get('live_trading_enabled')}",
        "",
        "## Diagnosis",
        "```json",
        json.dumps(payload.get("diagnosis") or {}, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Source artifacts",
    ]
    artifacts = payload.get("source_artifacts") or []
    if not artifacts:
        lines.append("- none")
    for artifact in artifacts:
        lines.append(f"- {artifact.get('path')} present={artifact.get('present')}")
    lines.extend(["", "## Allowed actions"])
    lines.extend([f"- {action}" for action in payload.get("allowed_actions") or []] or ["- none"])
    lines.extend(["", "## Forbidden actions"])
    lines.extend([f"- {action}" for action in payload.get("forbidden_actions") or []] or ["- none"])
    return "\n".join(lines) + "\n"


def write_state_files(payload: dict[str, Any], *, json_path: str | Path, markdown_path: str | Path) -> None:
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_out.write_text(hermes_correction_state_to_markdown(payload), encoding="utf-8")
