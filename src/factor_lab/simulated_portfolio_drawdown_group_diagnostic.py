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
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.md"

_GROUP_DIMENSIONS = ("holding_count", "rebalance_frequency", "cost_bps")


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


def _group_sort_key(item: tuple[str, Any]) -> tuple[int, float | str]:
    key = item[0]
    try:
        return (0, float(key))
    except ValueError:
        return (1, key)


def _summarize_rows(rows: list[dict[str, Any]], drawdown_limit: float) -> dict[str, Any]:
    drawdowns = [_float_or_none(row.get("max_drawdown")) for row in rows]
    sharpes = [_float_or_none(row.get("sharpe")) for row in rows]
    valid_drawdowns = [value for value in drawdowns if value is not None]
    valid_sharpes = [value for value in sharpes if value is not None]
    best_drawdown = round(max(valid_drawdowns), 6) if valid_drawdowns else None
    return {
        "result_count": len(rows),
        "best_max_drawdown": best_drawdown,
        "median_max_drawdown": round(float(statistics.median(valid_drawdowns)), 6) if valid_drawdowns else None,
        "best_sharpe": round(max(valid_sharpes), 6) if valid_sharpes else None,
        "pass_count_under_drawdown_limit": sum(1 for value in valid_drawdowns if value > drawdown_limit),
        "drawdown_gap_to_limit": round(drawdown_limit - best_drawdown, 6) if best_drawdown is not None else None,
    }


def summarize_drawdown_groups(matrix_payload: dict[str, Any], drawdown_limit: float) -> dict[str, Any]:
    ok_rows = _ok_results(matrix_payload)
    groups: dict[str, dict[str, dict[str, Any]]] = {}
    for dimension in _GROUP_DIMENSIONS:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in ok_rows:
            grouped.setdefault(str(row.get(dimension)), []).append(row)
        groups[dimension] = {
            key: _summarize_rows(rows, drawdown_limit) for key, rows in sorted(grouped.items(), key=_group_sort_key)
        }
    return {"ok_result_count": len(ok_rows), "groups": groups}


def _sort_float(value: Any, default: float = float("-inf")) -> float:
    numeric = _float_or_none(value)
    return numeric if numeric is not None else default


def _dimension_priority(dimension: Any) -> int:
    priorities = {"holding_count": 0, "rebalance_frequency": 1, "cost_bps": 2}
    return priorities.get(str(dimension), 99)


def _least_bad_axis(grouped_results: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for dimension, groups in (grouped_results.get("groups") or {}).items():
        if not isinstance(groups, dict):
            continue
        for value, stats in groups.items():
            if not isinstance(stats, dict):
                continue
            candidates.append({"dimension": dimension, "value": value, **stats})
    if not candidates:
        return None
    candidates.sort(
        key=lambda row: (
            -int(row.get("pass_count_under_drawdown_limit") or 0),
            _dimension_priority(row.get("dimension")),
            -_sort_float(row.get("best_max_drawdown")),
            -_sort_float(row.get("best_sharpe")),
            str(row.get("value")),
        )
    )
    return candidates[0]


def build_simulated_portfolio_drawdown_group_diagnostic(
    matrix_path: str | Path = DEFAULT_MATRIX_PATH,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    matrix_payload = load_matrix(matrix_path)
    policy = load_small_institutional_simulation_policy(policy_path)
    thresholds = policy.get("diagnosis_thresholds") or {}
    drawdown_limit = float(thresholds.get("max_drawdown_limit", -0.35))
    grouped = summarize_drawdown_groups(matrix_payload, drawdown_limit)
    recommended = _least_bad_axis(grouped)
    pass_count = sum(
        int(stats.get("pass_count_under_drawdown_limit") or 0)
        for dimension_groups in (grouped.get("groups") or {}).values()
        for stats in dimension_groups.values()
        if isinstance(stats, dict)
    )
    status = "manual_axis_review_ready" if pass_count > 0 else "blocked_no_group_under_drawdown_limit"
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "matrix_path": str(matrix_path),
        "matrix_status": matrix_payload.get("matrix_status"),
        "matrix_result_count": (matrix_payload.get("execution") or {}).get("result_count") or len(_ok_results(matrix_payload)),
        "drawdown_limit": drawdown_limit,
        "diagnostic_status": status,
        "recommended_manual_axis": recommended,
        "automation_allowed": False,
        "grouped_results": grouped,
    }


def drawdown_group_diagnostic_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Simulated Portfolio Drawdown Group Diagnostic",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"diagnostic_status: {payload.get('diagnostic_status')}",
        f"drawdown_limit: {payload.get('drawdown_limit')}",
        f"automation_allowed: {payload.get('automation_allowed')}",
        "",
        "## recommended_manual_axis",
    ]
    recommendation = payload.get("recommended_manual_axis")
    if recommendation:
        lines.extend(f"- {key}: {value}" for key, value in recommendation.items())
    else:
        lines.append("- none")

    grouped = payload.get("grouped_results") or {}
    lines.extend(["", "## Drawdown groups", f"- ok_result_count: {grouped.get('ok_result_count')}"])
    for dimension, groups in (grouped.get("groups") or {}).items():
        lines.append(f"### {dimension}")
        for value, stats in groups.items():
            lines.append(
                "- "
                f"{value}: result_count={stats.get('result_count')}, "
                f"best_max_drawdown={stats.get('best_max_drawdown')}, "
                f"median_max_drawdown={stats.get('median_max_drawdown')}, "
                f"best_sharpe={stats.get('best_sharpe')}, "
                f"pass_count_under_drawdown_limit={stats.get('pass_count_under_drawdown_limit')}, "
                f"drawdown_gap_to_limit={stats.get('drawdown_gap_to_limit')}"
            )
    return "\n".join(lines) + "\n"


def write_simulated_portfolio_drawdown_group_diagnostic(
    *,
    matrix_path: str | Path = DEFAULT_MATRIX_PATH,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_simulated_portfolio_drawdown_group_diagnostic(matrix_path, policy_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(drawdown_group_diagnostic_to_markdown(payload), encoding="utf-8")
    return payload
