from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.small_institutional_simulation_policy import load_small_institutional_simulation_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PREFLIGHT_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "dataset_preflight.json"
DEFAULT_MATRIX_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "self_diagnosis.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "self_diagnosis.md"


def _load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _cost_sensitivity_drop(results: list[dict[str, Any]]) -> float:
    by_label: dict[str, dict[float, float]] = {}
    for row in results:
        if row.get("status") != "ok":
            continue
        label = str(row.get("label") or "")
        cost = float(row.get("cost_bps") or 0.0)
        total_return = float(row.get("total_return") or 0.0)
        by_label.setdefault(label, {})[cost] = total_return
    drops: list[float] = []
    for cost_map in by_label.values():
        if not cost_map:
            continue
        zero = cost_map.get(0.0)
        high_cost = cost_map.get(max(cost_map))
        if zero is not None and high_cost is not None and abs(zero) > 1e-12:
            drops.append((zero - high_cost) / abs(zero))
    return max(drops, default=0.0)


def build_small_institutional_self_diagnosis(
    *,
    preflight_payload: dict[str, Any] | None = None,
    matrix_payload: dict[str, Any] | None = None,
    policy_path: str | Path | None = None,
) -> dict[str, Any]:
    policy = load_small_institutional_simulation_policy(policy_path) if policy_path else load_small_institutional_simulation_policy()
    thresholds = policy.get("diagnosis_thresholds") or {}
    preflight = preflight_payload or {}
    matrix = matrix_payload or {}
    summary = matrix.get("summary") or {}
    best = matrix.get("best_result") or {}
    results = matrix.get("results") or []

    result_count = int(summary.get("result_count") or 0)
    insufficient_count = int(summary.get("insufficient_data_count") or 0)
    insufficient_ratio = insufficient_count / result_count if result_count else 1.0
    max_insufficient_ratio = float(thresholds.get("max_insufficient_data_ratio", 0.25))
    max_drawdown_limit = float(thresholds.get("max_drawdown_limit", -0.35))
    min_sharpe = float(thresholds.get("min_sharpe", 0.8))
    max_cost_drop = float(thresholds.get("max_cost_sensitivity_drop", 0.25))
    best_drawdown = float(best.get("max_drawdown") or 0.0)
    best_sharpe = float(best.get("sharpe") or 0.0)
    cost_drop = _cost_sensitivity_drop(results)

    evidence: list[str] = []
    preflight_status = preflight.get("preflight_status") or "missing"
    matrix_status = matrix.get("matrix_status") or "missing"

    if preflight_status in {"blocked", "partial", "missing"} or insufficient_ratio > max_insufficient_ratio:
        evidence.append(f"preflight_status={preflight_status}")
        evidence.append(f"insufficient_ratio={insufficient_ratio:.3f}")
        status = "blocked"
        issue = "data_coverage_gap"
        severity = "high"
        next_action = "extend_backtest_dataset"
        automation_allowed = False
        run_mode = "preflight_only"
    elif matrix_status in {"missing", "insufficient_data"}:
        evidence.append(f"matrix_status={matrix_status}")
        status = "blocked"
        issue = "data_coverage_gap"
        severity = "high"
        next_action = "run_small_institutional_backtest_matrix"
        automation_allowed = False
        run_mode = "preflight_only"
    elif best_drawdown < max_drawdown_limit:
        evidence.append(f"best_max_drawdown={best_drawdown} < limit={max_drawdown_limit}")
        status = "blocked"
        issue = "drawdown_risk_too_high"
        severity = "high"
        next_action = "repair_simulated_portfolio_construction"
        automation_allowed = False
        run_mode = "bounded_matrix"
    elif cost_drop > max_cost_drop:
        evidence.append(f"cost_sensitivity_drop={cost_drop:.3f} > limit={max_cost_drop}")
        status = "watch"
        issue = "cost_sensitive_unstable"
        severity = "medium"
        next_action = "repair_cost_turnover_robustness"
        automation_allowed = False
        run_mode = "bounded_matrix"
    elif best_sharpe < min_sharpe:
        evidence.append(f"best_sharpe={best_sharpe} < min={min_sharpe}")
        status = "watch"
        issue = "weak_risk_adjusted_return"
        severity = "medium"
        next_action = "continue_simulated_research"
        automation_allowed = False
        run_mode = "bounded_matrix"
    else:
        evidence.append(f"best_sharpe={best_sharpe} >= min={min_sharpe}")
        evidence.append(f"best_max_drawdown={best_drawdown} >= limit={max_drawdown_limit}")
        status = "ready"
        issue = "ready_for_broader_simulation"
        severity = "low"
        next_action = "run_bounded_large_scale_simulation"
        automation_allowed = True
        run_mode = "large_scale_matrix"

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "diagnosis_status": status,
        "primary_issue": issue,
        "severity": severity,
        "next_action": next_action,
        "automation_allowed": automation_allowed,
        "recommended_run_mode": run_mode,
        "metrics": {
            "preflight_status": preflight_status,
            "matrix_status": matrix_status,
            "insufficient_ratio": round(insufficient_ratio, 6),
            "best_sharpe": best_sharpe,
            "best_max_drawdown": best_drawdown,
            "cost_sensitivity_drop": round(cost_drop, 6),
        },
        "thresholds": thresholds,
        "evidence": evidence,
    }


def small_institutional_self_diagnosis_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Small Institutional Self Diagnosis",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Diagnosis status: {payload.get('diagnosis_status')}",
        f"Primary issue: {payload.get('primary_issue')}",
        f"Severity: {payload.get('severity')}",
        f"Next action: {payload.get('next_action')}",
        f"Automation allowed: {payload.get('automation_allowed')}",
        f"Recommended run mode: {payload.get('recommended_run_mode')}",
        "",
        "## Evidence",
    ]
    evidence = payload.get("evidence") or []
    lines.extend(f"- {item}" for item in evidence) if evidence else lines.append("- none")
    lines.extend(["", "## Metrics"])
    for key, value in (payload.get("metrics") or {}).items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def write_small_institutional_self_diagnosis(
    *,
    preflight_path: str | Path = DEFAULT_PREFLIGHT_PATH,
    matrix_path: str | Path = DEFAULT_MATRIX_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    policy_path: str | Path | None = None,
) -> dict[str, Any]:
    payload = build_small_institutional_self_diagnosis(
        preflight_payload=_load_json(preflight_path),
        matrix_payload=_load_json(matrix_path),
        policy_path=policy_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(small_institutional_self_diagnosis_to_markdown(payload), encoding="utf-8")
    return payload
