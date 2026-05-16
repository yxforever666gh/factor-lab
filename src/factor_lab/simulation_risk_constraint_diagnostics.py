from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _best_available_drawdown(matrix: dict[str, Any], self_diagnosis: dict[str, Any]) -> float | None:
    best: float | None = None
    results = matrix.get("results") or []
    if isinstance(results, list):
        for row in results:
            if not isinstance(row, dict) or row.get("status", "ok") != "ok":
                continue
            drawdown = _float_or_none(row.get("max_drawdown"))
            if drawdown is None:
                continue
            if best is None or drawdown > best:
                best = drawdown
    if best is not None:
        return round(best, 6)

    for source in (matrix.get("best_result") or {}, self_diagnosis.get("metrics") or {}):
        if isinstance(source, dict):
            drawdown = _float_or_none(source.get("max_drawdown") or source.get("best_max_drawdown"))
            if drawdown is not None:
                return round(drawdown, 6)
    return None


def _drawdown_threshold(matrix: dict[str, Any], self_diagnosis: dict[str, Any], repair: dict[str, Any]) -> float | None:
    for source in (repair, self_diagnosis.get("thresholds") or {}, matrix.get("thresholds") or {}):
        if isinstance(source, dict):
            threshold = _float_or_none(source.get("drawdown_limit") or source.get("max_drawdown_limit"))
            if threshold is not None:
                return round(threshold, 6)
    return None


def _safe_next_step(best_drawdown: float | None, threshold: float | None, candidate_count: int) -> str:
    if best_drawdown is None or threshold is None:
        return "manual_review_missing_risk_inputs_before_rerun"
    if best_drawdown < threshold:
        if candidate_count <= 0:
            return "tighten_simulation_risk_constraints_before_rerun"
        return "reduce_sleeve_aggressiveness_before_rerun"
    return "manual_review_drawdown_safe_candidate_before_rerun"


def build_simulation_risk_constraint_diagnostics(
    matrix: dict[str, Any],
    self_diagnosis: dict[str, Any],
    repair: dict[str, Any],
) -> dict[str, Any]:
    best_drawdown = _best_available_drawdown(matrix, self_diagnosis)
    threshold = _drawdown_threshold(matrix, self_diagnosis, repair)
    if best_drawdown is None or threshold is None:
        gap = None
        status = "missing_risk_inputs"
    else:
        gap = round(abs(best_drawdown - threshold), 6) if best_drawdown < threshold else 0.0
        status = "blocked_drawdown_gap" if gap > 0 else "within_drawdown_threshold_manual_review"

    candidate_count = int(repair.get("candidate_count") or 0)
    recommended = _safe_next_step(best_drawdown, threshold, candidate_count)
    summary = matrix.get("summary") if isinstance(matrix.get("summary"), dict) else {}

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "diagnostic_status": status,
        "matrix_status": matrix.get("matrix_status"),
        "primary_issue": self_diagnosis.get("primary_issue"),
        "best_available_drawdown": best_drawdown,
        "drawdown_threshold": threshold,
        "drawdown_gap": gap,
        "candidate_count": candidate_count,
        "ok_result_count": summary.get("ok_count") or summary.get("ok_result_count"),
        "result_count": summary.get("result_count"),
        "repair_status": repair.get("repair_status"),
        "recommended_safe_next_step": recommended,
        "automation_allowed": False,
        "safety": {
            "queue_write_allowed": False,
            "daemon_change_allowed": False,
            "live_trading_enabled": False,
        },
    }


def diagnostics_to_markdown(payload: dict[str, Any]) -> str:
    safety = payload.get("safety") or {}
    lines = [
        "# Simulation Risk Constraint Diagnostics",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Diagnostic status: {payload.get('diagnostic_status')}",
        f"Matrix status: {payload.get('matrix_status')}",
        f"Primary issue: {payload.get('primary_issue')}",
        "",
        "## Drawdown gap",
        f"- best_available_drawdown: {payload.get('best_available_drawdown')}",
        f"- drawdown_threshold: {payload.get('drawdown_threshold')}",
        f"- drawdown_gap: {payload.get('drawdown_gap')}",
        f"- candidate_count: {payload.get('candidate_count')}",
        f"- repair_status: {payload.get('repair_status')}",
        "",
        "## Recommendation",
        f"- recommended_safe_next_step: {payload.get('recommended_safe_next_step')}",
        f"- automation_allowed: {payload.get('automation_allowed')}",
        "",
        "## Safety",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- daemon_change_allowed: {safety.get('daemon_change_allowed')}",
        f"- live_trading_enabled: {safety.get('live_trading_enabled')}",
    ]
    return "\n".join(lines) + "\n"


def write_simulation_risk_constraint_diagnostics(
    *,
    matrix_path: str | Path,
    self_diagnosis_path: str | Path,
    repair_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    payload = build_simulation_risk_constraint_diagnostics(
        load_json(matrix_path),
        load_json(self_diagnosis_path),
        load_json(repair_path),
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(diagnostics_to_markdown(payload), encoding="utf-8")
    return payload
