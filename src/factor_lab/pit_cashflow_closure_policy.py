from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "artifacts" / "pit_cashflow_conditioning" / "cashflow_closure_policy.json"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "pit_cashflow_conditioning" / "cashflow_conditioning_diagnostics.json"
CASHFLOW_FIELDS = {
    "operating_cashflow_to_profit",
    "operating_cashflow_to_profit_zscore_by_date_industry",
    "reversed_operating_cashflow_to_profit_zscore_by_date_industry",
}
DEFAULT_BLOCKED_ROUTE_IDS = {"value_trap_filter_quality_confirmation"}
DEFAULT_CLOSURE_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "decision": "cashflow_monitor_only_closed",
    "status": "monitor_only",
    "blocked_route_ids": sorted(DEFAULT_BLOCKED_ROUTE_IDS),
    "monitor_only_fields": sorted(CASHFLOW_FIELDS),
    "reopen_requires": [
        "new_written_mechanism_plan",
        "positive_read_only_evidence",
        "explicit_cashflow_reopen_plan_id_in_config",
    ],
    "evidence_status": "legacy_evidence_not_required_for_enforcement",
}


@dataclass(frozen=True)
class CashflowClosureDecision:
    decision: str
    reasons: tuple[str, ...]
    matched_fields: tuple[str, ...] = ()
    matched_route_id: str | None = None
    override: str | None = None

    @property
    def allowed(self) -> bool:
        return self.decision == "allow"

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reasons": list(self.reasons),
            "matched_fields": list(self.matched_fields),
            "matched_route_id": self.matched_route_id,
            "override": self.override,
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


def closure_policy_active(policy: Mapping[str, Any] | None) -> bool:
    if not policy:
        return False
    return str(policy.get("decision") or "") in {
        "stop_cashflow_conditioning_non_incremental",
        "cashflow_monitor_only_closed",
        "close_cashflow_value_trap_line",
    }


def load_cashflow_closure_policy(path: str | Path | None = None) -> dict[str, Any]:
    selected_path = Path(path) if path is not None else DEFAULT_POLICY_PATH
    payload = _read_json(selected_path)
    if payload:
        return payload
    # The closure is a governance rule, not a generated research conclusion.
    # Keep it enforceable in clean checkouts where ignored audit artifacts are
    # intentionally absent. Explicit non-default paths retain missing-file
    # semantics so callers can test or stage a different policy safely.
    if path is None or selected_path == DEFAULT_POLICY_PATH:
        return dict(DEFAULT_CLOSURE_POLICY)
    return {}


def _iter_factor_expressions(config: Mapping[str, Any]) -> Iterable[str]:
    for key in ("factors", "factor_definitions"):
        rows = config.get(key) or []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, Mapping):
                    expr = row.get("expression")
                    if expr:
                        yield str(expr)


def _field_mentions(config: Mapping[str, Any]) -> set[str]:
    text_parts: list[str] = []
    for key in ("required_data_fields", "required_pit_features"):
        values = config.get(key) or []
        if isinstance(values, list):
            text_parts.extend(str(v) for v in values)
    text_parts.extend(_iter_factor_expressions(config))
    joined = "\n".join(text_parts)
    return {field for field in CASHFLOW_FIELDS if field in joined}


def _closure_override(config: Mapping[str, Any]) -> str | None:
    governance = config.get("governance") or {}
    if isinstance(governance, Mapping):
        value = governance.get("cashflow_reopen_plan_id") or governance.get("cashflow_closure_override")
        if value:
            return str(value)
    value = config.get("cashflow_reopen_plan_id") or config.get("cashflow_closure_override")
    return str(value) if value else None


def evaluate_cashflow_closure(config: Mapping[str, Any], *, policy: Mapping[str, Any] | None = None) -> CashflowClosureDecision:
    policy = dict(policy or load_cashflow_closure_policy())
    if not closure_policy_active(policy):
        return CashflowClosureDecision("allow", ("closure_policy_not_active",))
    override = _closure_override(config)
    if override:
        return CashflowClosureDecision("allow", ("explicit_cashflow_reopen_plan",), override=override)

    blocked_routes = set(policy.get("blocked_route_ids") or DEFAULT_BLOCKED_ROUTE_IDS)
    route_id = str(config.get("route_id") or config.get("mechanism_id") or "")
    fields = sorted(_field_mentions(config))
    reasons: list[str] = []
    if route_id in blocked_routes:
        reasons.append("cashflow_value_trap_route_closed")
    if fields:
        reasons.append("cashflow_fields_monitor_only")
    if reasons:
        return CashflowClosureDecision("block", tuple(reasons), tuple(fields), route_id or None)
    return CashflowClosureDecision("allow", ("no_cashflow_closure_match",))


def build_cashflow_closure_policy(diagnostics: Mapping[str, Any] | None = None) -> dict[str, Any]:
    diagnostics = dict(diagnostics or _read_json(DEFAULT_DIAGNOSTICS_PATH))
    decision = str(diagnostics.get("decision") or "stop_cashflow_conditioning_non_incremental")
    payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "status": "monitor_only",
        "blocked_route_ids": sorted(DEFAULT_BLOCKED_ROUTE_IDS),
        "monitor_only_fields": sorted(CASHFLOW_FIELDS),
        "reopen_requires": [
            "new_written_mechanism_plan",
            "positive_read_only_evidence",
            "explicit_cashflow_reopen_plan_id_in_config",
        ],
        "evidence": {
            "baseline_spread": diagnostics.get("baseline", {}).get("bucket_pair_spread_mean") or diagnostics.get("baseline_spread"),
            "best_cashflow_conditioning_spread": diagnostics.get("best_cashflow_conditioning_spread"),
            "decision": decision,
            "source": "artifacts/pit_cashflow_conditioning/cashflow_conditioning_diagnostics.json",
        },
    }
    return payload


def closure_policy_to_markdown(policy: Mapping[str, Any]) -> str:
    lines = [
        "# PIT Cashflow Conditioning Closure Policy",
        "",
        f"Generated: {policy.get('generated_at_utc')}",
        f"Decision: {policy.get('decision')}",
        f"Status: {policy.get('status')}",
        "",
        "## Blocked route ids",
    ]
    for route in policy.get("blocked_route_ids") or []:
        lines.append(f"- {route}")
    lines += ["", "## Monitor-only fields"]
    for field in policy.get("monitor_only_fields") or []:
        lines.append(f"- {field}")
    lines += ["", "## Reopen requires"]
    for item in policy.get("reopen_requires") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_cashflow_closure_policy(
    *,
    json_path: str | Path = DEFAULT_POLICY_PATH,
    markdown_path: str | Path | None = None,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
) -> dict[str, Any]:
    policy = build_cashflow_closure_policy(_read_json(diagnostics_path))
    out_json = Path(json_path)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(policy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if markdown_path is None:
        markdown_path = out_json.with_suffix(".md")
    out_md = Path(markdown_path)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(closure_policy_to_markdown(policy), encoding="utf-8")
    return policy
