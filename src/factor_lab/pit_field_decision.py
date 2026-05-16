from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_value_trap_field_fix"
DEFAULT_FIELDS = ["operating_cashflow_to_profit", "debt_to_assets", "netprofit_yoy", "tr_yoy"]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _field_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row.get("field"): row for row in payload.get("fields", [])}


def _best_transform(row: dict[str, Any]) -> dict[str, Any] | None:
    variants = row.get("variants") or []
    if not variants:
        return None
    return max(variants, key=lambda v: -999 if v.get("rank_ic_mean") is None else float(v["rank_ic_mean"]))


def build_field_decision(
    *,
    cashflow: dict[str, Any],
    transforms: dict[str, Any],
    missing: dict[str, Any],
    fields: list[str] | None = None,
) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    transform_map = _field_map(transforms)
    missing_map = _field_map(missing)
    rows: list[dict[str, Any]] = []
    any_eligible = False
    cashflow_cov = float(cashflow.get("coverage") or 0.0)
    for field in fields:
        trow = transform_map.get(field, {})
        mrow = missing_map.get(field, {})
        best = _best_transform(trow) or {}
        best_ic = best.get("rank_ic_mean")
        best_variant = best.get("variant")
        coverage = float(best.get("coverage") or 0.0)
        fragility = mrow.get("fragility")
        reasons: list[str] = []
        action = "drop"
        eligible_for_combo = False
        if field == "operating_cashflow_to_profit" and cashflow_cov < 0.30:
            reasons.append("coverage_below_30pct_cashflow_hard_stop")
            action = "monitor_only"
        elif coverage < 0.30:
            reasons.append("coverage_below_30pct")
            action = "monitor_only"
        elif best_ic is None:
            reasons.append("no_usable_ic")
            action = "drop"
        elif float(best_ic) <= 0.005:
            reasons.append("best_direction_ic_too_weak")
            action = "drop" if fragility == "unusable" else "monitor_only"
        elif fragility in {"direction_changes_by_missing_treatment", "single_treatment_positive_only"}:
            reasons.append(f"fragile_missing_treatment:{fragility}")
            action = "transform_only"
        elif best_variant and "reversed" in best_variant:
            action = "reverse"
            eligible_for_combo = True
        elif best_variant and "winsorized" in best_variant:
            action = "transform_only"
            eligible_for_combo = True
        else:
            action = "keep"
            eligible_for_combo = True
        if coverage < 0.60:
            reasons.append("coverage_below_preferred_60pct")
        if action in {"keep", "reverse", "transform_only"} and eligible_for_combo:
            any_eligible = True
        rows.append({
            "field": field,
            "action": action,
            "eligible_for_repaired_combo": bool(eligible_for_combo and action in {"keep", "reverse", "transform_only"}),
            "best_variant": best_variant,
            "best_rank_ic_mean": best_ic,
            "coverage": coverage,
            "missing_value_fragility": fragility,
            "reasons": reasons,
        })
    decision = "allow_repaired_config_generation" if any_eligible else "stop_value_trap_repair_no_fields_clear_gates"
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "fields": rows,
        "eligible_fields": [r["field"] for r in rows if r["eligible_for_repaired_combo"]],
        "hard_stops": [
            "raw_pit_financial_hard_add_forbidden",
            *(["operating_cashflow_to_profit_coverage_below_30pct"] if cashflow_cov < 0.30 else []),
        ],
    }


def decision_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Field Decision", "", f"Generated: {payload.get('generated_at_utc')}", f"Decision: {payload.get('decision')}", ""]
    for row in payload.get("fields", []):
        lines.append(f"## {row['field']}")
        lines.append(f"- action: {row['action']}")
        lines.append(f"- eligible_for_repaired_combo: {row['eligible_for_repaired_combo']}")
        lines.append(f"- best_variant: {row['best_variant']}")
        lines.append(f"- best_rank_ic_mean: {row['best_rank_ic_mean']}")
        lines.append(f"- coverage: {row['coverage']}")
        lines.append(f"- missing_value_fragility: {row['missing_value_fragility']}")
        lines.append(f"- reasons: {', '.join(row['reasons']) if row['reasons'] else 'none'}")
        lines.append("")
    return "\n".join(lines)


def write_field_decision(output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir = Path(output_dir)
    payload = build_field_decision(
        cashflow=_load_json(output_dir / "cashflow_coverage_diagnostics.json"),
        transforms=_load_json(output_dir / "field_transform_diagnostics.json"),
        missing=_load_json(output_dir / "missing_value_diagnostics.json"),
    )
    (output_dir / "field_decision.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "field_decision.md").write_text(decision_to_markdown(payload), encoding="utf-8")
    return payload
