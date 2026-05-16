from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from factor_lab.data_coverage_preflight import build_mechanism_data_gap_report
from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS
from factor_lab.value_factor_templates import build_value_route_candidates

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACT_DIR = ROOT / "artifacts"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _gate_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_decision = Counter(str(row.get("decision") or "unknown") for row in rows)
    by_bucket = Counter(str(row.get("budget_bucket") or "unknown") for row in rows)
    reason_counter: Counter[str] = Counter()
    for row in rows:
        for reason in row.get("reasons") or []:
            reason_counter[str(reason)] += 1
    return {
        "total": len(rows),
        "by_decision": dict(by_decision),
        "by_budget_bucket": dict(by_bucket),
        "top_reasons": [{"reason": reason, "count": count} for reason, count in reason_counter.most_common(20)],
    }


def build_research_quality_summary(
    *,
    gate_decision_path: str | Path | None = None,
    available_fields: Iterable[str] | None = None,
    controlled_ledger_summary_path: str | Path | None = None,
    value_sleeve_decision_path: str | Path | None = None,
    value_sleeve_policy_path: str | Path | None = None,
) -> dict[str, Any]:
    fields = set(available_fields or TUSHARE_FEATURE_COLUMNS)
    gate_path = Path(gate_decision_path) if gate_decision_path else DEFAULT_ARTIFACT_DIR / "research_gate_decisions.jsonl"
    ledger_path = Path(controlled_ledger_summary_path) if controlled_ledger_summary_path else DEFAULT_ARTIFACT_DIR / "controlled_run_ledger_summary.json"
    sleeve_path = Path(value_sleeve_decision_path) if value_sleeve_decision_path else DEFAULT_ARTIFACT_DIR / "value_sleeve_validation" / "value_sleeve_decision.json"
    sleeve_policy_path = Path(value_sleeve_policy_path) if value_sleeve_policy_path else DEFAULT_ARTIFACT_DIR / "value_sleeve_validation" / "value_sleeve_policy.json"
    value_routes = build_value_route_candidates(available_fields=fields)
    ready_routes = [row for row in value_routes if row.get("status") == "ready"]
    blocked_routes = [row for row in value_routes if row.get("status") != "ready"]
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "gate_decisions": _gate_summary(_read_jsonl(gate_path)),
        "data_coverage": build_mechanism_data_gap_report(available_fields=fields),
        "value_research_routes": {
            "ready": ready_routes,
            "blocked": blocked_routes,
            "ready_count": len(ready_routes),
            "blocked_count": len(blocked_routes),
        },
        "controlled_runtime": _read_json(ledger_path),
        "value_sleeve_validation": _read_json(sleeve_path),
        "value_sleeve_policy": _read_json(sleeve_policy_path),
    }


def _to_markdown(payload: dict[str, Any]) -> str:
    gate = payload.get("gate_decisions", {})
    value = payload.get("value_research_routes", {})
    coverage = payload.get("data_coverage", {}).get("summary", {})
    controlled = payload.get("controlled_runtime") or {}
    sleeve = payload.get("value_sleeve_validation") or {}
    sleeve_policy = payload.get("value_sleeve_policy") or {}
    lines = [
        "# Research Quality Summary",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        "",
        "## Gate decisions",
        f"- Total: {gate.get('total', 0)}",
        f"- By decision: {gate.get('by_decision', {})}",
        f"- By budget bucket: {gate.get('by_budget_bucket', {})}",
        "",
        "## Data coverage",
        f"- Ready templates: {coverage.get('ready_templates', 0)}",
        f"- Blocked templates: {coverage.get('blocked_templates', 0)}",
        f"- Missing fields: {coverage.get('missing_fields', [])}",
        "",
        "## Controlled runtime",
        f"- Total controlled runs: {controlled.get('total', 0)}",
        f"- By status: {controlled.get('by_status', {})}",
        f"- Main blockers: {controlled.get('main_blockers', {})}",
        "",
        "## Value sleeve validation",
        f"- Decision: {sleeve.get('decision', 'not_available')}",
        f"- Primary route: {sleeve.get('primary_route')}",
        f"- Confirmation route: {sleeve.get('confirmation_route')}",
        f"- Recommended next action: {sleeve.get('recommended_next_action')}",
        "- Broad daemon restoration remains forbidden.",
        "",
        "## Value sleeve policy",
        f"- Decision: {sleeve_policy.get('decision', 'not_available')}",
        f"- Primary route: {sleeve_policy.get('primary_route')}",
        f"- Confirmation route: {sleeve_policy.get('confirmation_route')}",
        f"- Low-weight route: {sleeve_policy.get('low_weight_route')}",
        "",
        "## Value research routes",
        f"- Ready candidates: {value.get('ready_count', 0)}",
        f"- Blocked routes: {value.get('blocked_count', 0)}",
    ]
    for row in value.get("blocked", [])[:10]:
        lines.append(f"  - {row.get('route_id')}: missing {row.get('missing_fields')}")
    return "\n".join(lines) + "\n"


def write_research_quality_summary(
    *,
    json_path: str | Path,
    markdown_path: str | Path | None = None,
    available_fields: Iterable[str] | None = None,
    gate_decision_path: str | Path | None = None,
    controlled_ledger_summary_path: str | Path | None = None,
    value_sleeve_decision_path: str | Path | None = None,
    value_sleeve_policy_path: str | Path | None = None,
) -> dict[str, Any]:
    payload = build_research_quality_summary(
        gate_decision_path=gate_decision_path,
        available_fields=available_fields,
        controlled_ledger_summary_path=controlled_ledger_summary_path,
        value_sleeve_decision_path=value_sleeve_decision_path,
        value_sleeve_policy_path=value_sleeve_policy_path,
    )
    out_json = Path(json_path)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if markdown_path:
        out_md = Path(markdown_path)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text(_to_markdown(payload), encoding="utf-8")
    return payload
