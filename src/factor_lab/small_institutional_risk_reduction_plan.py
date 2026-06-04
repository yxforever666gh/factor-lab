from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.small_institutional_backtest_matrix import deterministic_combo_id
from factor_lab.small_institutional_simulation_policy import load_small_institutional_simulation_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "configs" / "small_institutional_simulation_policy.json"
DEFAULT_MATRIX_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.json"
DEFAULT_DATASET_PATH = ROOT / "artifacts" / "value_route_bucket_aware" / "runs" / "value_quality_no_distress_bucket_aware" / "dataset.csv"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_plan.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_plan.md"

_REQUIRED_RISK_FILTER_FIELDS = ["volatility_20", "turnover"]
_OPTIONAL_RISK_FILTER_FIELDS = ["volatility_60", "roe"]
_FORBIDDEN_ACTIONS = [
    "no_broad_daemon_restore",
    "no_queue_write",
    "no_timer_enable",
    "no_auto_promotion",
    "no_live_trading",
    "no_drawdown_limit_relaxation",
]


def _load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _dataset_columns(path: str | Path) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            return [column.strip() for column in next(reader)]
        except StopIteration:
            return []


def _risk_filters(available: set[str]) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        {
            "field": "volatility_20",
            "operator": "lte_quantile",
            "quantile": 0.70,
            "rationale": "reduce recent realized-volatility tail exposure before expanding simulation",
        },
        {
            "field": "turnover",
            "operator": "lte_quantile",
            "quantile": 0.80,
            "rationale": "avoid high-turnover names while preserving enough breadth",
        },
    ]
    if "volatility_60" in available:
        filters.append(
            {
                "field": "volatility_60",
                "operator": "lte_quantile",
                "quantile": 0.75,
                "rationale": "confirm lower medium-horizon volatility exposure",
            }
        )
    if "roe" in available:
        filters.append(
            {
                "field": "roe",
                "operator": "gte_quantile",
                "quantile": 0.30,
                "rationale": "exclude weakest profitability tail without turning this into a new alpha route",
            }
        )
    return filters


def _executed_combo_ids(matrix_payload: dict[str, Any]) -> set[str]:
    return {str(row.get("combo_id")) for row in matrix_payload.get("results") or [] if isinstance(row, dict) and row.get("combo_id")}


def _seen_signals(matrix_payload: dict[str, Any]) -> set[str]:
    return {str(row.get("signal_column")) for row in matrix_payload.get("results") or [] if isinstance(row, dict) and row.get("signal_column")}


def _candidate_id(spec: dict[str, Any]) -> str:
    canonical = json.dumps(spec, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _windows(policy: dict[str, Any]) -> list[dict[str, str]]:
    windows = policy.get("year_windows") or []
    return [window for window in windows if isinstance(window, dict) and window.get("start_date") and window.get("end_date")]


def _sort_key(spec: dict[str, Any], seen_signals: set[str]) -> tuple[int, int, int, str]:
    signal = str(spec.get("signal_column"))
    # Prefer signals not represented in the capped matrix, then 75/100 holdings,
    # then realistic lower cost. Keep deterministic lexical fallback.
    unseen_priority = 0 if signal not in seen_signals else 1
    holding = int(spec.get("holding_count") or 0)
    holding_priority = 0 if holding in {75, 100} else 1
    cost = int(float(spec.get("cost_bps") or 0))
    return (unseen_priority, holding_priority, cost, spec.get("combo_id", ""))


def build_risk_reduction_plan(
    *,
    policy: dict[str, Any],
    matrix_payload: dict[str, Any],
    available_columns: list[str] | set[str],
    max_next_backtests: int = 120,
    source_dataset_path: str | Path | None = None,
) -> dict[str, Any]:
    available = set(available_columns)
    missing = [field for field in _REQUIRED_RISK_FILTER_FIELDS if field not in available]
    thresholds = policy.get("diagnosis_thresholds") or {}
    drawdown_limit = float(thresholds.get("max_drawdown_limit", -0.35))
    generated_at = datetime.now(timezone.utc).isoformat()
    base_payload: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "plan_type": "small_institutional_risk_reduction_manual_review",
        "source_dataset_path": str(source_dataset_path) if source_dataset_path is not None else None,
        "drawdown_limit": drawdown_limit,
        "max_next_backtests": int(max_next_backtests),
        "automation_allowed": False,
        "manual_review_required": True,
        "queue_write_allowed": False,
        "live_trading_enabled": False,
        "forbidden_actions": list(_FORBIDDEN_ACTIONS),
        "required_risk_filter_fields": list(_REQUIRED_RISK_FILTER_FIELDS),
        "optional_risk_filter_fields": list(_OPTIONAL_RISK_FILTER_FIELDS),
        "available_risk_filter_fields": [field for field in _REQUIRED_RISK_FILTER_FIELDS + _OPTIONAL_RISK_FILTER_FIELDS if field in available],
        "missing_risk_filter_fields": missing,
    }
    if missing:
        base_payload.update(
            {
                "plan_status": "blocked_missing_risk_filter_fields",
                "candidate_count": 0,
                "candidate_specs": [],
            }
        )
        return base_payload

    executed = _executed_combo_ids(matrix_payload)
    seen_signals = _seen_signals(matrix_payload)
    filters = _risk_filters(available)
    signals = [str(signal) for signal in policy.get("signal_columns") or []]
    holding_counts = [int(value) for value in policy.get("holding_counts") or [75, 100]]
    # Risk-reduction follow-up should use realistic costs only; zero-cost rows are diagnostic, not deployment-like.
    cost_values = [float(value) for value in policy.get("cost_bps_values") or [30, 60] if float(value) > 0]
    frequencies = [str(value) for value in policy.get("rebalance_frequencies") or ["monthly"]]
    return_column = str(policy.get("return_column") or "forward_return_5d")
    specs: list[dict[str, Any]] = []
    for signal in signals:
        for window in _windows(policy):
            for holding in holding_counts:
                for frequency in frequencies:
                    for cost in cost_values:
                        combo = {
                            "signal_column": signal,
                            "label": window.get("label") or f"{window['start_date']}:{window['end_date']}",
                            "start_date": window["start_date"],
                            "end_date": window["end_date"],
                            "holding_count": holding,
                            "rebalance_frequency": frequency,
                            "cost_bps": cost,
                        }
                        combo["combo_id"] = deterministic_combo_id(combo)
                        if combo["combo_id"] in executed:
                            continue
                        spec = {
                            **combo,
                            "candidate_id": "",
                            "source": "risk_reduction_manual_review",
                            "return_column": return_column,
                            "risk_filters": filters,
                            "execution_allowed": False,
                            "manual_review_required": True,
                        }
                        spec["candidate_id"] = _candidate_id({k: v for k, v in spec.items() if k != "candidate_id"})
                        specs.append(spec)
    specs = sorted(specs, key=lambda item: _sort_key(item, seen_signals))[: max(0, int(max_next_backtests))]
    base_payload.update(
        {
            "plan_status": "candidate_plan_ready" if specs else "blocked_no_unexecuted_candidate_specs",
            "candidate_count": len(specs),
            "candidate_specs": specs,
            "matrix_execution": matrix_payload.get("execution") or {},
        }
    )
    return base_payload


def risk_reduction_plan_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Small Institutional Risk Reduction Plan",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"plan_status: {payload.get('plan_status')}",
        f"candidate_count: {payload.get('candidate_count')}",
        f"drawdown_limit: {payload.get('drawdown_limit')}",
        f"automation_allowed: {payload.get('automation_allowed')}",
        f"manual_review_required: {payload.get('manual_review_required')}",
        f"queue_write_allowed: {payload.get('queue_write_allowed')}",
        f"live_trading_enabled: {payload.get('live_trading_enabled')}",
        "",
        "## Missing risk filter fields",
    ]
    missing = payload.get("missing_risk_filter_fields") or []
    lines.extend([f"- {field}" for field in missing] or ["- none"])
    lines.extend(["", "## Forbidden actions"])
    lines.extend([f"- {action}" for action in payload.get("forbidden_actions") or []])
    lines.extend(["", "## Candidate specs"])
    specs = payload.get("candidate_specs") or []
    if not specs:
        lines.append("- none")
    for spec in specs[:20]:
        filters = ", ".join(f"{item.get('field')} {item.get('operator')} {item.get('quantile')}" for item in spec.get("risk_filters") or [])
        lines.append(
            "- "
            f"{spec.get('candidate_id')} signal={spec.get('signal_column')} label={spec.get('label')} "
            f"holdings={spec.get('holding_count')} freq={spec.get('rebalance_frequency')} cost={spec.get('cost_bps')} filters=[{filters}]"
        )
    return "\n".join(lines) + "\n"


def write_risk_reduction_plan(
    *,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    matrix_path: str | Path = DEFAULT_MATRIX_PATH,
    dataset_path: str | Path | None = None,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    max_next_backtests: int = 120,
) -> dict[str, Any]:
    policy = load_small_institutional_simulation_policy(policy_path)
    matrix_payload = _load_json(matrix_path)
    effective_dataset_path = dataset_path or policy.get("dataset_path") or DEFAULT_DATASET_PATH
    payload = build_risk_reduction_plan(
        policy=policy,
        matrix_payload=matrix_payload,
        available_columns=_dataset_columns(effective_dataset_path),
        max_next_backtests=max_next_backtests,
        source_dataset_path=effective_dataset_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(risk_reduction_plan_to_markdown(payload), encoding="utf-8")
    return payload
