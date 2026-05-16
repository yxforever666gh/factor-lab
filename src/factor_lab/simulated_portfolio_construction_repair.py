from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.small_institutional_simulation_policy import load_small_institutional_simulation_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MATRIX_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.json"
DEFAULT_POLICY_PATH = ROOT / "configs" / "small_institutional_simulation_policy.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.md"

_GROUP_DIMENSIONS = ("signal_column", "holding_count", "rebalance_frequency", "cost_bps")


def load_matrix(path: str | Path) -> dict[str, Any]:
    matrix_path = Path(path)
    if not matrix_path.exists():
        return {}
    try:
        payload = json.loads(matrix_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _ok_results(matrix_payload: dict[str, Any]) -> list[dict[str, Any]]:
    results = matrix_payload.get("results") or []
    if not isinstance(results, list):
        return []
    return [row for row in results if isinstance(row, dict) and row.get("status") == "ok"]


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _summarize_rows(rows: list[dict[str, Any]], drawdown_limit: float) -> dict[str, Any]:
    sharpes = [_float_or_none(row.get("sharpe")) for row in rows]
    drawdowns = [_float_or_none(row.get("max_drawdown")) for row in rows]
    valid_sharpes = [value for value in sharpes if value is not None]
    valid_drawdowns = [value for value in drawdowns if value is not None]
    return {
        "result_count": len(rows),
        "best_sharpe": round(max(valid_sharpes), 6) if valid_sharpes else None,
        "best_max_drawdown": round(max(valid_drawdowns), 6) if valid_drawdowns else None,
        "median_max_drawdown": round(float(statistics.median(valid_drawdowns)), 6) if valid_drawdowns else None,
        "pass_count_under_drawdown_limit": sum(1 for value in valid_drawdowns if value >= drawdown_limit),
    }


def group_matrix_results(matrix_payload: dict[str, Any], drawdown_limit: float) -> dict[str, Any]:
    ok_rows = _ok_results(matrix_payload)
    dimension_summaries: dict[str, dict[str, Any]] = {}
    for dimension in _GROUP_DIMENSIONS:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in ok_rows:
            key = str(row.get(dimension))
            grouped.setdefault(key, []).append(row)
        dimension_summaries[dimension] = {
            key: _summarize_rows(rows, drawdown_limit) for key, rows in sorted(grouped.items(), key=lambda item: item[0])
        }
    return {
        "ok_result_count": len(ok_rows),
        "dimension_summaries": dimension_summaries,
    }


def _candidate_projection(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "combo_id",
        "signal_column",
        "label",
        "start_date",
        "end_date",
        "holding_count",
        "rebalance_frequency",
        "cost_bps",
        "sharpe",
        "max_drawdown",
        "total_return",
        "turnover_mean",
    ]
    return {key: row.get(key) for key in keys if key in row}


def _safe_candidates(matrix_payload: dict[str, Any], drawdown_limit: float) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in _ok_results(matrix_payload):
        drawdown = _float_or_none(row.get("max_drawdown"))
        if drawdown is not None and drawdown >= drawdown_limit:
            candidates.append(row)
    return sorted(
        candidates,
        key=lambda row: (
            _float_or_none(row.get("max_drawdown")) or float("-inf"),
            _float_or_none(row.get("sharpe")) or float("-inf"),
            _float_or_none(row.get("total_return")) or float("-inf"),
        ),
        reverse=True,
    )


def build_simulated_portfolio_construction_repair(
    matrix_path: str | Path = DEFAULT_MATRIX_PATH,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    matrix_payload = load_matrix(matrix_path)
    policy = load_small_institutional_simulation_policy(policy_path)
    thresholds = policy.get("diagnosis_thresholds") or {}
    drawdown_limit = float(thresholds.get("max_drawdown_limit", -0.35))
    grouped = group_matrix_results(matrix_payload, drawdown_limit)
    safe = _safe_candidates(matrix_payload, drawdown_limit)
    recommended = _candidate_projection(safe[0]) if safe else None
    repair_status = "candidate_found" if safe else "blocked_no_drawdown_safe_candidate"
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "matrix_path": str(matrix_path),
        "drawdown_limit": drawdown_limit,
        "repair_status": repair_status,
        "candidate_count": len(safe),
        "recommended_candidate": recommended,
        "automation_allowed": False,
        "grouped_results": grouped,
    }


def repair_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Simulated Portfolio Construction Repair",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"repair_status: {payload.get('repair_status')}",
        f"drawdown_limit: {payload.get('drawdown_limit')}",
        f"candidate_count: {payload.get('candidate_count')}",
        f"automation_allowed: {payload.get('automation_allowed')}",
        "",
        "## Recommended candidate",
    ]
    candidate = payload.get("recommended_candidate")
    if candidate:
        lines.extend(f"- {key}: {value}" for key, value in candidate.items())
    else:
        lines.append("- none")

    grouped = payload.get("grouped_results") or {}
    lines.extend(["", "## Dimension summaries", f"- ok_result_count: {grouped.get('ok_result_count')}"])
    for dimension, summary in (grouped.get("dimension_summaries") or {}).items():
        lines.append(f"### {dimension}")
        for key, stats in summary.items():
            lines.append(
                "- "
                f"{key}: result_count={stats.get('result_count')}, "
                f"best_sharpe={stats.get('best_sharpe')}, "
                f"best_max_drawdown={stats.get('best_max_drawdown')}, "
                f"median_max_drawdown={stats.get('median_max_drawdown')}, "
                f"pass_count_under_drawdown_limit={stats.get('pass_count_under_drawdown_limit')}"
            )
    return "\n".join(lines) + "\n"


def write_simulated_portfolio_construction_repair(
    *,
    matrix_path: str | Path = DEFAULT_MATRIX_PATH,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_simulated_portfolio_construction_repair(matrix_path, policy_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(repair_to_markdown(payload), encoding="utf-8")
    return payload
