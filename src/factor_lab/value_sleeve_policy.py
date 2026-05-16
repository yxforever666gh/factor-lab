from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIR = ROOT / "artifacts" / "value_sleeve_validation"
DEFAULT_DECISION_PATH = DEFAULT_DIR / "value_sleeve_decision.json"
DEFAULT_SCORECARD_PATH = DEFAULT_DIR / "route_scorecard.json"
DEFAULT_POLICY_PATH = DEFAULT_DIR / "value_sleeve_policy.json"
DEFAULT_MARKDOWN_PATH = DEFAULT_DIR / "value_sleeve_policy.md"

PRIMARY_ROUTE = "value_quality_no_distress"
CONFIRMATION_ROUTE = "value_momentum_confirmation"
LOW_WEIGHT_ROUTE = "industry_relative_value"

NO_POLICY_ACTION = {
    "role": "unclassified",
    "action": "no_sleeve_policy",
    "admission_rank": 99,
    "reason": "value_sleeve_policy_missing_or_not_applicable",
}


def _read_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _scorecard_routes(scorecard: dict[str, Any]) -> dict[str, Any]:
    routes = scorecard.get("routes")
    if isinstance(routes, dict):
        return routes
    rows = scorecard.get("scorecard") or scorecard.get("rows") or []
    if isinstance(rows, list):
        return {str(row.get("route_id")): row for row in rows if isinstance(row, dict) and row.get("route_id")}
    return {}


def build_value_sleeve_policy(
    *,
    decision_path: str | Path = DEFAULT_DECISION_PATH,
    scorecard_path: str | Path = DEFAULT_SCORECARD_PATH,
) -> dict[str, Any]:
    decision = _read_json(decision_path)
    scorecard = _read_json(scorecard_path)
    decision_name = str(decision.get("decision") or "no_sleeve_policy")
    if decision_name != "collapse_to_value_sleeve_with_primary_route":
        return {
            "schema_version": 1,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "no_sleeve_policy",
            "sleeve_id": "value",
            "routes": {},
            "constraints": ["do_not_restore_broad_daemon"],
            "reason": "missing_or_non_collapse_value_sleeve_decision",
        }

    primary = str(decision.get("primary_route") or PRIMARY_ROUTE)
    confirmation = str(decision.get("confirmation_route") or CONFIRMATION_ROUTE)
    low_weight = str(decision.get("low_weight_route") or decision.get("low_weight_core_route") or LOW_WEIGHT_ROUTE)
    score_routes = _scorecard_routes(scorecard)
    routes = {
        primary: {
            "role": "primary",
            "action": "prioritize_primary",
            "admission_rank": 0,
            "reason": "best_single_route_spread_and_sleeve_primary",
            "scorecard": score_routes.get(primary, {}),
        },
        confirmation: {
            "role": "confirmation",
            "action": "confirmation_only",
            "admission_rank": 1,
            "reason": "use_for_explicit_confirmation_not_equal_capacity",
            "scorecard": score_routes.get(confirmation, {}),
        },
        low_weight: {
            "role": "low_weight_core_value",
            "action": "cap_or_skip_duplicate",
            "admission_rank": 2,
            "reason": "high_duplicate_risk_and_weaker_spread",
            "scorecard": score_routes.get(low_weight, {}),
        },
    }
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision_name,
        "sleeve_id": "value",
        "primary_route": primary,
        "confirmation_route": confirmation,
        "low_weight_route": low_weight,
        "routes": routes,
        "constraints": [
            "do_not_restore_broad_daemon",
            "do_not_enqueue_more_single_route_followups",
            "do_not_treat_correlated_routes_as_independent_alpha",
        ],
    }


def load_value_sleeve_policy(path: str | Path = DEFAULT_POLICY_PATH) -> dict[str, Any]:
    payload = _read_json(path)
    return payload if payload else {"decision": "no_sleeve_policy", "routes": {}}


def route_sleeve_action(route_id: str | None, policy: dict[str, Any] | None) -> dict[str, Any]:
    if not route_id or not isinstance(policy, dict):
        return dict(NO_POLICY_ACTION)
    routes = policy.get("routes") or {}
    row = routes.get(str(route_id)) if isinstance(routes, dict) else None
    if not isinstance(row, dict):
        return dict(NO_POLICY_ACTION)
    return {
        "role": row.get("role", "unclassified"),
        "action": row.get("action", "no_sleeve_policy"),
        "admission_rank": int(row.get("admission_rank", 99)),
        "reason": row.get("reason", ""),
    }


def policy_to_markdown(policy: dict[str, Any]) -> str:
    routes = policy.get("routes") or {}
    lines = [
        "# Value Sleeve Policy",
        "",
        f"Generated: {policy.get('generated_at_utc')}",
        f"Decision: {policy.get('decision')}",
        f"Primary route: {policy.get('primary_route')}",
        f"Confirmation route: {policy.get('confirmation_route')}",
        f"Low-weight route: {policy.get('low_weight_route')}",
        "",
        "## Route actions",
    ]
    for route_id, row in routes.items():
        lines.append(f"- {route_id}: role={row.get('role')}, action={row.get('action')}, rank={row.get('admission_rank')}")
    lines.extend(["", "## Constraints"])
    for item in policy.get("constraints") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_value_sleeve_policy(
    *,
    json_path: str | Path = DEFAULT_POLICY_PATH,
    markdown_path: str | Path | None = DEFAULT_MARKDOWN_PATH,
    decision_path: str | Path = DEFAULT_DECISION_PATH,
    scorecard_path: str | Path = DEFAULT_SCORECARD_PATH,
) -> dict[str, Any]:
    policy = build_value_sleeve_policy(decision_path=decision_path, scorecard_path=scorecard_path)
    out_json = Path(json_path)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(policy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if markdown_path:
        out_md = Path(markdown_path)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text(policy_to_markdown(policy), encoding="utf-8")
    return policy
