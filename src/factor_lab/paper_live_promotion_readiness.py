from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_CONSTRAINT_HARDENING_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_constraint_hardening.json"
DEFAULT_PORTFOLIO_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_RETROSPECTIVE_TRACKING_PATH = ROOT / "artifacts" / "paper_portfolio" / "retrospective_return_tracking.json"
DEFAULT_DRY_RUN_PATH = ROOT / "artifacts" / "controlled_restart_dry_run.json"
DEFAULT_RUNTIME_AUDIT_PATH = ROOT / "artifacts" / "runtime_takeover_audit.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "paper_portfolio" / "paper_live_promotion_readiness.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "paper_portfolio" / "paper_live_promotion_readiness.md"


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _paper_observation_count(retrospective_tracking: dict[str, Any]) -> int:
    if "observation_count" in retrospective_tracking:
        return _to_int(retrospective_tracking.get("observation_count"), 0)
    portfolio_return = retrospective_tracking.get("portfolio_return") or {}
    return 1 if retrospective_tracking.get("tracking_status") == "ok" and portfolio_return.get("portfolio_forward_return") is not None else 0


def _minimum_observations(status: dict[str, Any]) -> int:
    policy = status.get("policy") or {}
    value = policy.get("paper_live_min_observations")
    if value is None:
        # Conservative default for the current MVP: one completed paper retrospective is enough for manual review, not live trading.
        return 1
    return _to_int(value, 1)


def _runtime_safe(status: dict[str, Any], dry_run: dict[str, Any], runtime_audit: dict[str, Any]) -> bool:
    status_runtime = status.get("runtime_safety") or {}
    recommendations = runtime_audit.get("recommendations") or status_runtime.get("recommendations") or []
    return bool(status_runtime.get("safe", True)) and "pause_broad_daemon" in recommendations and "allow_controlled_only_daemon" in recommendations


def evaluate_paper_live_promotion_readiness(
    status: dict[str, Any],
    constraint_hardening: dict[str, Any],
    portfolio: dict[str, Any],
    retrospective_tracking: dict[str, Any],
    dry_run: dict[str, Any],
    runtime_audit: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []

    constraint_status = constraint_hardening.get("constraint_status")
    if constraint_status != "pass":
        blockers.append("constraint_hardening_not_passed")

    would_run_count = _to_int(dry_run.get("would_run_count"), 0)
    claimable_count = _to_int(dry_run.get("claimable_workflow_count", dry_run.get("allowed_workflow_count")), would_run_count)
    if not _runtime_safe(status, dry_run, runtime_audit):
        blockers.append("runtime_not_controlled_safe")
    if would_run_count > 0 or claimable_count > 0:
        blockers.append("claimable_workflows_not_empty")

    actual_observations = _paper_observation_count(retrospective_tracking)
    minimum_observations = _minimum_observations(status)
    paper_observations_passed = actual_observations >= minimum_observations
    if not paper_observations_passed:
        warnings.append("insufficient_paper_observations")

    tracking_status = retrospective_tracking.get("tracking_status") or "missing"
    if tracking_status != "ok":
        warnings.append("retrospective_tracking_not_ok")

    if blockers:
        readiness_status = "blocked"
    elif warnings:
        readiness_status = "wait"
    else:
        readiness_status = "ready_for_manual_approval"

    return {
        "readiness_status": readiness_status,
        "blockers": blockers,
        "warnings": warnings,
        "manual_approval_required": True,
        "live_trading_enabled": False,
        "checks": {
            "constraint_hardening": {"passed": constraint_status == "pass", "constraint_status": constraint_status or "missing"},
            "runtime_safety": {"passed": "runtime_not_controlled_safe" not in blockers, "recommendations": runtime_audit.get("recommendations") or []},
            "claimable_workflows": {"passed": "claimable_workflows_not_empty" not in blockers, "would_run_count": would_run_count, "claimable_workflow_count": claimable_count},
            "paper_observations": {"passed": paper_observations_passed, "actual": actual_observations, "minimum": minimum_observations},
            "retrospective_tracking": {"passed": tracking_status == "ok", "tracking_status": tracking_status},
        },
        "portfolio": {
            "strategy_name": portfolio.get("strategy_name"),
            "as_of_date": portfolio.get("as_of_date"),
            "position_count": portfolio.get("position_count"),
        },
    }


def build_paper_live_promotion_readiness(
    *,
    status_path: str | Path = DEFAULT_STATUS_PATH,
    constraint_hardening_path: str | Path = DEFAULT_CONSTRAINT_HARDENING_PATH,
    portfolio_path: str | Path = DEFAULT_PORTFOLIO_PATH,
    retrospective_tracking_path: str | Path = DEFAULT_RETROSPECTIVE_TRACKING_PATH,
    dry_run_path: str | Path = DEFAULT_DRY_RUN_PATH,
    runtime_audit_path: str | Path = DEFAULT_RUNTIME_AUDIT_PATH,
) -> dict[str, Any]:
    status = load_json(status_path)
    constraint = load_json(constraint_hardening_path)
    portfolio = load_json(portfolio_path)
    tracking = load_json(retrospective_tracking_path)
    dry_run = load_json(dry_run_path)
    audit = load_json(runtime_audit_path)
    evaluation = evaluate_paper_live_promotion_readiness(status, constraint, portfolio, tracking, dry_run, audit)
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        **evaluation,
        "inputs": {
            "status_path": str(status_path),
            "constraint_hardening_path": str(constraint_hardening_path),
            "portfolio_path": str(portfolio_path),
            "retrospective_tracking_path": str(retrospective_tracking_path),
            "dry_run_path": str(dry_run_path),
            "runtime_audit_path": str(runtime_audit_path),
        },
    }


def paper_live_promotion_readiness_to_markdown(payload: dict[str, Any]) -> str:
    portfolio = payload.get("portfolio") or {}
    checks = payload.get("checks") or {}
    lines = [
        "# Paper/Live Promotion Readiness",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Readiness status: {payload.get('readiness_status')}",
        f"Manual approval required: {payload.get('manual_approval_required')}",
        f"Live trading enabled: {payload.get('live_trading_enabled')}",
        f"Blockers: {payload.get('blockers') or []}",
        f"Warnings: {payload.get('warnings') or []}",
        "",
        "## Portfolio",
        f"- Strategy: {portfolio.get('strategy_name')}",
        f"- As-of date: {portfolio.get('as_of_date')}",
        f"- Position count: {portfolio.get('position_count')}",
        "",
        "## Checks",
    ]
    for name, check in checks.items():
        lines.append(f"- {name}: passed={check.get('passed')} details={check}")
    lines.extend(
        [
            "",
            "## Safety note",
            "- This artifact is read-only and cannot execute live trades.",
            "- Any live trading discussion still requires explicit manual approval.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_paper_live_promotion_readiness(
    *,
    status_path: str | Path = DEFAULT_STATUS_PATH,
    constraint_hardening_path: str | Path = DEFAULT_CONSTRAINT_HARDENING_PATH,
    portfolio_path: str | Path = DEFAULT_PORTFOLIO_PATH,
    retrospective_tracking_path: str | Path = DEFAULT_RETROSPECTIVE_TRACKING_PATH,
    dry_run_path: str | Path = DEFAULT_DRY_RUN_PATH,
    runtime_audit_path: str | Path = DEFAULT_RUNTIME_AUDIT_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_paper_live_promotion_readiness(
        status_path=status_path,
        constraint_hardening_path=constraint_hardening_path,
        portfolio_path=portfolio_path,
        retrospective_tracking_path=retrospective_tracking_path,
        dry_run_path=dry_run_path,
        runtime_audit_path=runtime_audit_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(paper_live_promotion_readiness_to_markdown(payload), encoding="utf-8")
    return payload
