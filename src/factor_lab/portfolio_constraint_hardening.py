from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "configs" / "small_institutionalization_policy.json"
DEFAULT_PORTFOLIO_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_RETROSPECTIVE_TRACKING_PATH = ROOT / "artifacts" / "paper_portfolio" / "retrospective_return_tracking.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_constraint_hardening.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_constraint_hardening.md"


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _position_count(portfolio: dict[str, Any]) -> int:
    explicit = portfolio.get("position_count")
    try:
        if explicit is not None:
            return int(explicit)
    except (TypeError, ValueError):
        pass
    positions = portfolio.get("positions") or []
    return len(positions) if isinstance(positions, list) else 0


def _max_position_weight(portfolio: dict[str, Any]) -> float | None:
    constraints = portfolio.get("constraints") or {}
    if constraints.get("max_position_weight") is not None:
        return _to_float(constraints.get("max_position_weight"), 0.0)
    positions = portfolio.get("positions") or []
    if not isinstance(positions, list) or not positions:
        return None
    weights = [_to_float(row.get("weight"), None) for row in positions if isinstance(row, dict)]
    weights = [weight for weight in weights if weight is not None]
    return max(weights) if weights else None


def _check_position_count(policy: dict[str, Any], portfolio: dict[str, Any]) -> dict[str, Any]:
    actual = _position_count(portfolio)
    minimum = int(policy.get("target_holdings_min") or 0)
    maximum = int(policy.get("target_holdings_max") or 10**9)
    passed = minimum <= actual <= maximum
    return {"passed": passed, "actual": actual, "min": minimum, "max": maximum}


def _check_single_name_weight_cap(policy: dict[str, Any], portfolio: dict[str, Any]) -> dict[str, Any]:
    constraints = policy.get("portfolio_constraints_next_phase") or {}
    cap = _to_float(constraints.get("single_name_weight_cap"), None)
    actual = _max_position_weight(portfolio)
    if cap is None:
        return {"passed": True, "actual": actual, "cap": None, "reason": "cap_not_configured"}
    passed = actual is not None and actual <= cap + 1e-12
    return {"passed": passed, "actual": round(float(actual or 0.0), 6), "cap": cap}


def _check_benchmark(policy: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, Any]:
    benchmark = diagnostics.get("benchmark") or {}
    actual = benchmark.get("benchmark_id")
    allowed = policy.get("benchmark_candidates") or []
    passed = bool(actual) and (not allowed or actual in allowed)
    return {"passed": passed, "actual": actual, "allowed": allowed, "tracking_mode": benchmark.get("tracking_mode")}


def _check_turnover(policy: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, Any]:
    constraints = policy.get("portfolio_constraints_next_phase") or {}
    budget = _to_float(constraints.get("turnover_budget_per_rebalance"), None)
    actual = _to_float((diagnostics.get("turnover") or {}).get("turnover_one_way_estimate"), None)
    if budget is None or actual is None:
        return {"passed": True, "actual": actual, "budget": budget, "reason": "turnover_budget_or_actual_missing"}
    return {"passed": actual <= budget + 1e-12, "actual": actual, "budget": budget}


def _check_retrospective_tracking(retrospective_tracking: dict[str, Any]) -> dict[str, Any]:
    returns = retrospective_tracking.get("portfolio_return") or {}
    status = retrospective_tracking.get("tracking_status")
    return {
        "passed": status == "ok",
        "tracking_status": status or "missing",
        "portfolio_forward_return": returns.get("portfolio_forward_return"),
        "matched_position_count": returns.get("matched_position_count"),
        "missing_position_count": returns.get("missing_position_count"),
    }


def evaluate_portfolio_constraints(
    policy: dict[str, Any],
    current_portfolio: dict[str, Any],
    diagnostics: dict[str, Any],
    retrospective_tracking: dict[str, Any],
) -> dict[str, Any]:
    checks = {
        "position_count": _check_position_count(policy, current_portfolio),
        "single_name_weight_cap": _check_single_name_weight_cap(policy, current_portfolio),
        "benchmark": _check_benchmark(policy, diagnostics),
        "turnover_budget": _check_turnover(policy, diagnostics),
        "retrospective_tracking": _check_retrospective_tracking(retrospective_tracking),
    }
    violations: list[str] = []
    warnings: list[str] = []

    if not checks["retrospective_tracking"]["passed"]:
        warnings.append("retrospective_tracking_not_ready")
    if not checks["position_count"]["passed"]:
        violations.append("position_count_out_of_range")
    if not checks["single_name_weight_cap"]["passed"]:
        violations.append("single_name_weight_cap_breached")
    if not checks["benchmark"]["passed"]:
        violations.append("benchmark_missing_or_not_allowed")
    if not checks["turnover_budget"]["passed"]:
        violations.append("turnover_budget_breached")

    if warnings and not violations:
        status = "wait"
    elif violations:
        status = "fail"
    else:
        status = "pass"

    return {
        "constraint_status": status,
        "violations": violations,
        "warnings": warnings,
        "checks": checks,
    }


def build_portfolio_constraint_hardening(
    *,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    portfolio_path: str | Path = DEFAULT_PORTFOLIO_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    retrospective_tracking_path: str | Path = DEFAULT_RETROSPECTIVE_TRACKING_PATH,
) -> dict[str, Any]:
    policy = load_json(policy_path)
    portfolio = load_json(portfolio_path)
    diagnostics = load_json(diagnostics_path)
    tracking = load_json(retrospective_tracking_path)
    evaluation = evaluate_portfolio_constraints(policy, portfolio, diagnostics, tracking)
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        **evaluation,
        "portfolio": {
            "strategy_name": portfolio.get("strategy_name"),
            "as_of_date": portfolio.get("as_of_date"),
            "position_count": _position_count(portfolio),
            "max_position_weight": _max_position_weight(portfolio),
        },
        "inputs": {
            "policy_path": str(policy_path),
            "portfolio_path": str(portfolio_path),
            "diagnostics_path": str(diagnostics_path),
            "retrospective_tracking_path": str(retrospective_tracking_path),
        },
    }


def portfolio_constraint_hardening_to_markdown(payload: dict[str, Any]) -> str:
    checks = payload.get("checks") or {}
    portfolio = payload.get("portfolio") or {}
    lines = [
        "# Portfolio Constraint Hardening",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Constraint status: {payload.get('constraint_status')}",
        f"Violations: {payload.get('violations') or []}",
        f"Warnings: {payload.get('warnings') or []}",
        "",
        "## Portfolio",
        f"- Strategy: {portfolio.get('strategy_name')}",
        f"- As-of date: {portfolio.get('as_of_date')}",
        f"- Position count: {portfolio.get('position_count')}",
        f"- Max position weight: {portfolio.get('max_position_weight')}",
        "",
        "## Checks",
    ]
    for name, check in checks.items():
        lines.append(f"- {name}: passed={check.get('passed')} actual={check.get('actual', check.get('tracking_status'))}")
    return "\n".join(lines) + "\n"


def write_portfolio_constraint_hardening(
    *,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    portfolio_path: str | Path = DEFAULT_PORTFOLIO_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    retrospective_tracking_path: str | Path = DEFAULT_RETROSPECTIVE_TRACKING_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_portfolio_constraint_hardening(
        policy_path=policy_path,
        portfolio_path=portfolio_path,
        diagnostics_path=diagnostics_path,
        retrospective_tracking_path=retrospective_tracking_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(portfolio_constraint_hardening_to_markdown(payload), encoding="utf-8")
    return payload
