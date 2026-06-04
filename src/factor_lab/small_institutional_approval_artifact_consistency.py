from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT_ORDER = ["manual_review", "gate", "operator_summary", "status"]
SAFETY_FLAGS = [
    "queue_write_allowed",
    "broad_daemon_allowed",
    "automation_allowed",
    "automated_rerun_allowed",
    "live_trading_enabled",
]


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _decision_axis(dimension: Any, value: Any) -> str | None:
    if dimension is None or value is None:
        return None
    return f"{dimension}={value}"


def _manual_review_fields(manual_review: dict[str, Any]) -> dict[str, Any]:
    decision = manual_review.get("recommended_manual_decision") or {}
    safety = manual_review.get("safety") or {}
    return {
        "primary_blocker": manual_review.get("primary_issue"),
        "decision_axis": _decision_axis(decision.get("dimension"), decision.get("value")),
        "repair_status": manual_review.get("repair_status"),
        "best_available_max_drawdown": manual_review.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": manual_review.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(decision.get("automated_rerun_allowed")),
        "live_trading_enabled": False,
    }


def _gate_fields(gate: dict[str, Any]) -> dict[str, Any]:
    required = gate.get("required_approval") or {}
    safety = gate.get("safety") or {}
    return {
        "primary_blocker": required.get("primary_issue"),
        "decision_axis": _decision_axis(required.get("dimension"), required.get("value")),
        "repair_status": required.get("repair_status"),
        "best_available_max_drawdown": required.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": required.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(gate.get("queue_write_allowed") or safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(gate.get("broad_daemon_allowed") or safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(gate.get("automation_allowed") or safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(gate.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(gate.get("live_trading_enabled")),
    }


def _operator_summary_fields(summary: dict[str, Any]) -> dict[str, Any]:
    safety = summary.get("safety") or {}
    return {
        "primary_blocker": summary.get("primary_blocker"),
        "decision_axis": summary.get("required_decision_axis"),
        "repair_status": summary.get("repair_status"),
        "best_available_max_drawdown": summary.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": summary.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(safety.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(safety.get("live_trading_enabled")),
    }


def _status_fields(status: dict[str, Any]) -> dict[str, Any]:
    review = status.get("repair_blocker_manual_review") or {}
    gate = status.get("manual_approval_gate") or {}
    summary = status.get("operator_approval_summary") or {}
    return {
        "primary_blocker": review.get("primary_issue") or summary.get("primary_blocker"),
        "decision_axis": _decision_axis(review.get("manual_decision_dimension"), review.get("manual_decision_value"))
        or summary.get("required_decision_axis"),
        "repair_status": review.get("repair_status"),
        "best_available_max_drawdown": review.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": review.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(review.get("queue_write_allowed") or gate.get("queue_write_allowed") or summary.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(review.get("broad_daemon_allowed") or gate.get("broad_daemon_allowed") or summary.get("broad_daemon_allowed")),
        "automation_allowed": bool(review.get("automation_allowed") or gate.get("automation_allowed") or summary.get("automation_allowed")),
        "automated_rerun_allowed": bool(
            review.get("automated_rerun_allowed") or gate.get("automated_rerun_allowed") or summary.get("automated_rerun_allowed")
        ),
        "live_trading_enabled": bool(gate.get("live_trading_enabled") or summary.get("live_trading_enabled")),
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _staleness_warnings(artifacts: dict[str, dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    timestamps: dict[str, datetime | None] = {}
    for name in ARTIFACT_ORDER:
        value = artifacts[name].get("generated_at_utc")
        parsed = _parse_timestamp(value)
        timestamps[name] = parsed
        if value is None:
            warnings.append(f"{name}:missing_generated_at_utc")
        elif parsed is None:
            warnings.append(f"{name}:invalid_generated_at_utc")

    previous_name: str | None = None
    previous_ts: datetime | None = None
    for name in ARTIFACT_ORDER:
        current_ts = timestamps[name]
        if previous_name and previous_ts and current_ts and current_ts < previous_ts:
            warnings.append(f"{name}_older_than_{previous_name}")
        if current_ts is not None:
            previous_name = name
            previous_ts = current_ts
    return warnings


def _compare_fields(sources: dict[str, dict[str, Any]], field_names: list[str]) -> tuple[dict[str, Any], list[str]]:
    matched: dict[str, Any] = {}
    inconsistencies: list[str] = []
    for field_name in field_names:
        values = {name: fields.get(field_name) for name, fields in sources.items() if fields.get(field_name) is not None}
        unique_values = {json.dumps(value, sort_keys=True, ensure_ascii=False) for value in values.values()}
        if len(unique_values) <= 1:
            matched[field_name] = next(iter(values.values()), None)
        else:
            inconsistencies.append(f"{field_name} mismatch: {values}")
    return matched, inconsistencies


def build_approval_artifact_consistency(
    *,
    manual_review_path: str | Path,
    gate_path: str | Path,
    operator_summary_path: str | Path,
    status_path: str | Path,
) -> dict[str, Any]:
    manual_review = load_json(manual_review_path)
    gate = load_json(gate_path)
    operator_summary = load_json(operator_summary_path)
    status = load_json(status_path)
    artifacts = {
        "manual_review": manual_review,
        "gate": gate,
        "operator_summary": operator_summary,
        "status": status,
    }
    sources = {
        "manual_review": _manual_review_fields(manual_review),
        "gate": _gate_fields(gate),
        "operator_summary": _operator_summary_fields(operator_summary),
        "status": _status_fields(status),
    }
    matched_fields, inconsistencies = _compare_fields(
        sources,
        ["primary_blocker", "decision_axis", "repair_status", "best_available_max_drawdown", "drawdown_gap_to_limit"],
    )
    safety_flags, safety_inconsistencies = _compare_fields(sources, SAFETY_FLAGS)
    inconsistencies.extend(safety_inconsistencies)
    missing = [name for name, payload in artifacts.items() if not payload]
    inconsistencies.extend(f"{name} missing" for name in missing)

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "consistency_status": "ok" if not inconsistencies else "inconsistent",
        "non_mutating": True,
        "matched_fields": matched_fields,
        "safety_flags": safety_flags,
        "inconsistencies": inconsistencies,
        "staleness_warnings": _staleness_warnings(artifacts),
        "source_artifacts": {
            "manual_review_path": str(manual_review_path),
            "gate_path": str(gate_path),
            "operator_summary_path": str(operator_summary_path),
            "status_path": str(status_path),
        },
    }


def approval_artifact_consistency_to_markdown(payload: dict[str, Any]) -> str:
    matched = payload.get("matched_fields") or {}
    safety = payload.get("safety_flags") or {}
    inconsistencies = payload.get("inconsistencies") or []
    warnings = payload.get("staleness_warnings") or []
    lines = [
        "# Approval Artifact Consistency",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Consistency status: {payload.get('consistency_status')}",
        f"Non-mutating: {payload.get('non_mutating')}",
        "",
        "## Matched fields",
    ]
    lines.extend(f"- {key}: {value}" for key, value in matched.items())
    lines.extend(["", "## Safety flags"])
    lines.extend(f"- {key}: {value}" for key, value in safety.items())
    lines.extend(["", "## Inconsistencies"])
    lines.extend(f"- {item}" for item in inconsistencies) if inconsistencies else lines.append("- none")
    lines.extend(["", "## Staleness warnings"])
    lines.extend(f"- {item}" for item in warnings) if warnings else lines.append("- none")
    return "\n".join(lines) + "\n"


def write_approval_artifact_consistency(
    *,
    manual_review_path: str | Path,
    gate_path: str | Path,
    operator_summary_path: str | Path,
    status_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    payload = build_approval_artifact_consistency(
        manual_review_path=manual_review_path,
        gate_path=gate_path,
        operator_summary_path=operator_summary_path,
        status_path=status_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(approval_artifact_consistency_to_markdown(payload), encoding="utf-8")
    return payload
