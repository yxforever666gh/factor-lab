from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.pit_financial_schema import PIT_FINANCIAL_FIELDS
from factor_lab.pit_value_trap_attribution import _bucket_spread_for_field, _rank_ic_summary

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET = ROOT / "artifacts" / "pit_cashflow_source_audit" / "diagnostic_dataset.csv"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_non_cashflow_mechanism_preflight"
RETURN_FIELD = "forward_return_5d"

BASE_MARKET_OR_LEGACY_FIELDS = {"roe"}
CASHFLOW_FIELDS = {"operating_cashflow_to_profit", "free_cashflow_to_assets"}


@dataclass(frozen=True)
class MechanismSpec:
    mechanism_id: str
    label: str
    question: str
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...] = ()
    allow_degraded_variant: bool = False
    degraded_variant_id: str | None = None
    degraded_required_fields: tuple[str, ...] = ()


MECHANISMS: tuple[MechanismSpec, ...] = (
    MechanismSpec(
        mechanism_id="accrual_quality",
        label="Accrual quality",
        question="Do cheap stocks with lower accrual burden perform better?",
        required_fields=("free_cashflow_to_assets", "operating_cashflow_to_profit", "debt_to_assets"),
        optional_fields=("netprofit_yoy",),
    ),
    MechanismSpec(
        mechanism_id="earnings_stability",
        label="Earnings stability",
        question="Do cheap stocks with more stable earnings avoid value traps?",
        required_fields=("netprofit_yoy", "tr_yoy"),
        optional_fields=("roe",),
    ),
    MechanismSpec(
        mechanism_id="balance_sheet_distress",
        label="Balance-sheet distress",
        question="Do cheap stocks with weaker leverage/liquidity/solvency underperform?",
        required_fields=("debt_to_assets", "current_ratio", "quick_ratio"),
        optional_fields=("roe",),
        allow_degraded_variant=True,
        degraded_variant_id="balance_sheet_distress_debt_only",
        degraded_required_fields=("debt_to_assets",),
    ),
    MechanismSpec(
        mechanism_id="profitability_margin_quality",
        label="Profitability persistence / margin quality",
        question="Do cheap companies with persistent profitability/margins perform better than one-off rebound names?",
        required_fields=("roe", "grossprofit_margin", "netprofit_margin"),
        optional_fields=("netprofit_yoy", "tr_yoy"),
    ),
)


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _financial_schema_by_name() -> dict[str, dict[str, Any]]:
    return {field.name: field.to_dict() for field in PIT_FINANCIAL_FIELDS}


def _is_usable_pit_field(field: str, df: pd.DataFrame) -> tuple[bool, str | None]:
    if field in CASHFLOW_FIELDS:
        return False, "cashflow_field_closed_or_excluded_from_non_cashflow_preflight"
    if field in BASE_MARKET_OR_LEGACY_FIELDS:
        return False, "ambiguous_legacy_market_column_not_confirmed_as_pit_surfaced"
    if field not in df.columns:
        return False, "not_surfaced_in_dataset_or_pit_cache"
    return True, None


def build_field_inventory(df: pd.DataFrame) -> dict[str, Any]:
    schema = _financial_schema_by_name()
    used_fields = sorted({f for m in MECHANISMS for f in (*m.required_fields, *m.optional_fields, *m.degraded_required_fields)})
    rows: list[dict[str, Any]] = []
    for field in used_fields:
        meta = schema.get(field, {})
        exists = field in df.columns
        usable, blocked_reason = _is_usable_pit_field(field, df)
        coverage = float(_numeric(df[field]).notna().mean()) if exists and len(df) else 0.0
        rows.append(
            {
                "field": field,
                "exists_in_dataset": exists,
                "usable_for_non_cashflow_pit_preflight": usable,
                "blocked_reason": blocked_reason,
                "coverage": coverage,
                "group": meta.get("group"),
                "source_table": meta.get("source_table"),
                "preferred_sources": meta.get("preferred_sources", []),
                "source_fields": meta.get("source_fields", []),
                "requires_pit": meta.get("requires_pit", True),
                "disclosure_date_policy": meta.get("disclosure_date_policy"),
            }
        )
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "rows": rows}


def _inventory_map(inventory: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["field"]: row for row in inventory.get("rows", [])}


def build_coverage_preflight(df: pd.DataFrame, inventory: dict[str, Any]) -> dict[str, Any]:
    inv = _inventory_map(inventory)
    mechanisms: list[dict[str, Any]] = []
    for spec in MECHANISMS:
        required = list(spec.required_fields)
        field_rows = [inv.get(field, {"field": field, "coverage": 0.0, "usable_for_non_cashflow_pit_preflight": False}) for field in required]
        missing = [row["field"] for row in field_rows if not row.get("usable_for_non_cashflow_pit_preflight")]
        complete_case = _complete_case_coverage(df, [f for f in required if inv.get(f, {}).get("usable_for_non_cashflow_pit_preflight")])
        decision = "ready" if not missing and complete_case >= 0.60 else "blocked"
        reasons = []
        if missing:
            reasons.append("missing_or_unusable_required_fields")
        if complete_case < 0.60:
            reasons.append("complete_case_coverage_below_60pct")
        degraded = None
        if spec.allow_degraded_variant and spec.degraded_variant_id:
            degraded_fields = list(spec.degraded_required_fields)
            degraded_missing = [f for f in degraded_fields if not inv.get(f, {}).get("usable_for_non_cashflow_pit_preflight")]
            degraded_coverage = _complete_case_coverage(df, degraded_fields if not degraded_missing else [])
            degraded = {
                "mechanism_id": spec.degraded_variant_id,
                "required_fields": degraded_fields,
                "missing_or_unusable_fields": degraded_missing,
                "complete_case_coverage": degraded_coverage,
                "decision": "ready_for_direction_preflight" if not degraded_missing and degraded_coverage >= 0.60 else "blocked",
            }
        mechanisms.append(
            {
                "mechanism_id": spec.mechanism_id,
                "label": spec.label,
                "question": spec.question,
                "required_fields": required,
                "optional_fields": list(spec.optional_fields),
                "field_coverage": {row["field"]: row.get("coverage", 0.0) for row in field_rows},
                "missing_or_unusable_fields": missing,
                "complete_case_coverage": complete_case,
                "decision": decision,
                "reasons": reasons,
                "degraded_variant": degraded,
            }
        )
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "mechanisms": mechanisms}


def _complete_case_coverage(df: pd.DataFrame, fields: list[str]) -> float:
    if not fields or df.empty:
        return 0.0
    existing = [f for f in fields if f in df.columns]
    if len(existing) != len(fields):
        return 0.0
    return float(df[existing].apply(pd.to_numeric, errors="coerce").notna().all(axis=1).mean())


def _variant_values(df: pd.DataFrame, field: str, variant: str) -> pd.Series:
    raw = _numeric(df[field])
    if variant == "raw":
        return raw
    if variant == "reversed":
        return -raw
    raise ValueError(variant)


def build_direction_preflight(df: pd.DataFrame, inventory: dict[str, Any]) -> dict[str, Any]:
    inv = _inventory_map(inventory)
    fields = [field for field, row in inv.items() if row.get("usable_for_non_cashflow_pit_preflight") and field in df.columns]
    rows: list[dict[str, Any]] = []
    for field in sorted(fields):
        variants = []
        for variant in ("raw", "reversed"):
            tmp = df[["date", RETURN_FIELD]].copy()
            tmp[field] = _variant_values(df, field, variant)
            ic = _rank_ic_summary(tmp, field, RETURN_FIELD)
            bucket = _bucket_spread_for_field(tmp, field, RETURN_FIELD)
            coverage = float(tmp[field].notna().mean()) if len(tmp) else 0.0
            variants.append(
                {
                    "variant": variant,
                    "coverage": coverage,
                    "rank_ic_mean": ic.get("rank_ic_mean"),
                    "rank_ic_ir": ic.get("rank_ic_ir"),
                    "ic_observations": ic.get("observations"),
                    "top_bottom_spread_mean": bucket.get("top_bottom_spread_mean"),
                    "bottom_top_spread_mean": bucket.get("bottom_top_spread_mean"),
                    "bucket_observations": bucket.get("observations"),
                }
            )
        best = max(variants, key=lambda row: float(row.get("rank_ic_mean") or -999.0))
        enough = best.get("rank_ic_mean") is not None and float(best["rank_ic_mean"]) >= 0.01 and float(best.get("top_bottom_spread_mean") or -999.0) > 0
        rows.append(
            {
                "field": field,
                "best_variant": best["variant"],
                "best_rank_ic_mean": best.get("rank_ic_mean"),
                "best_top_bottom_spread_mean": best.get("top_bottom_spread_mean"),
                "passes_direction_preflight": bool(enough),
                "variants": variants,
            }
        )
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def _load_prior_low_debt_evidence(root: Path = ROOT) -> dict[str, Any]:
    candidates = [
        root / "artifacts" / "pit_value_trap_field_fix" / "runs" / "repaired_debt_to_assets_reverse" / "bucket_aware_portfolio_results.json",
        root / "artifacts" / "pit_value_trap_field_fix" / "run_repaired_debt_to_assets_reverse" / "bucket_aware_portfolio_results.json",
        root / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation" / "bucket_aware_portfolio_results.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = payload if isinstance(payload, list) else [payload]
        if any(str(row.get("factor_name") or "").startswith("repaired_debt_to_assets_reverse") for row in rows if isinstance(row, dict)):
            return {"path": str(path), "payload": payload}
    return {}


def build_mechanism_selection_decision(
    inventory: dict[str, Any],
    coverage: dict[str, Any],
    direction: dict[str, Any],
    *,
    root: Path = ROOT,
) -> dict[str, Any]:
    direction_by_field = {row["field"]: row for row in direction.get("fields", [])}
    candidates: list[dict[str, Any]] = []
    for mech in coverage.get("mechanisms", []):
        if mech.get("decision") == "ready":
            fields = mech.get("required_fields", [])
            field_passes = [direction_by_field.get(f, {}).get("passes_direction_preflight", False) for f in fields]
            candidates.append({"mechanism_id": mech["mechanism_id"], "variant": "full", "fields": fields, "passes": all(field_passes)})
        degraded = mech.get("degraded_variant") or {}
        if degraded.get("decision") == "ready_for_direction_preflight":
            fields = degraded.get("required_fields", [])
            field_passes = [direction_by_field.get(f, {}).get("passes_direction_preflight", False) for f in fields]
            candidates.append({"mechanism_id": degraded["mechanism_id"], "variant": "degraded", "fields": fields, "passes": all(field_passes)})

    prior_low_debt = _load_prior_low_debt_evidence(root)
    reasons: list[str] = []
    accepted = [c for c in candidates if c.get("passes")]
    if prior_low_debt:
        reasons.append("prior_controlled_low_debt_probe_failed_or_non_incremental")
        accepted = [c for c in accepted if c.get("mechanism_id") != "balance_sheet_distress_debt_only"]
    if not accepted:
        decision = "stop_pit_value_trap_expansion_no_non_cashflow_mechanism_passed_preflight"
        recommended = None
    else:
        decision = "recommend_single_non_cashflow_controlled_probe_plan"
        recommended = accepted[0]
    if not candidates:
        reasons.append("no_mechanism_has_required_fields_and_coverage")
    else:
        failed = [c["mechanism_id"] for c in candidates if not c.get("passes")]
        if failed:
            reasons.append("direction_preflight_failed:" + ",".join(failed))
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "recommended_mechanism": recommended,
        "candidate_mechanisms": candidates,
        "prior_low_debt_evidence": prior_low_debt,
        "reasons": reasons,
        "hard_stops": [
            "no_queue_write",
            "no_workflow_run",
            "no_cashflow_conditioning",
            "no_broad_daemon_restore",
            "at_most_one_future_probe_only_if_separate_plan",
        ],
    }


def _write_json_md(output_dir: Path, stem: str, payload: dict[str, Any], md: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / f"{stem}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"{stem}.md").write_text(md, encoding="utf-8")


def _fmt(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.6f}"
    return str(v)


def inventory_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Non-Cashflow Field Inventory", "", f"Generated: {payload.get('generated_at_utc')}", ""]
    for row in payload.get("rows", []):
        lines.append(f"- {row['field']}: exists={row['exists_in_dataset']}, usable={row['usable_for_non_cashflow_pit_preflight']}, coverage={_fmt(row['coverage'])}, reason={row.get('blocked_reason')}")
    return "\n".join(lines) + "\n"


def coverage_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Non-Cashflow Coverage Preflight", "", f"Generated: {payload.get('generated_at_utc')}", ""]
    for mech in payload.get("mechanisms", []):
        lines.append(f"## {mech['mechanism_id']}")
        lines.append(f"Decision: {mech['decision']}")
        lines.append(f"Complete-case coverage: {_fmt(mech['complete_case_coverage'])}")
        lines.append(f"Missing/unusable: {', '.join(mech.get('missing_or_unusable_fields', [])) or 'none'}")
        degraded = mech.get("degraded_variant")
        if degraded:
            lines.append(f"Degraded variant {degraded['mechanism_id']}: {degraded['decision']}, coverage={_fmt(degraded['complete_case_coverage'])}")
        lines.append("")
    return "\n".join(lines)


def direction_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Non-Cashflow Direction Preflight", "", f"Generated: {payload.get('generated_at_utc')}", ""]
    for row in payload.get("fields", []):
        lines.append(f"## {row['field']}")
        lines.append(f"Best: {row['best_variant']} IC={_fmt(row.get('best_rank_ic_mean'))} spread={_fmt(row.get('best_top_bottom_spread_mean'))} pass={row['passes_direction_preflight']}")
        for v in row.get("variants", []):
            lines.append(f"- {v['variant']}: coverage={_fmt(v['coverage'])}, IC={_fmt(v.get('rank_ic_mean'))}, spread={_fmt(v.get('top_bottom_spread_mean'))}")
        lines.append("")
    return "\n".join(lines)


def decision_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Non-Cashflow Mechanism Selection Decision", "", f"Generated: {payload.get('generated_at_utc')}", "", f"Decision: {payload.get('decision')}", ""]
    lines.append(f"Recommended mechanism: {payload.get('recommended_mechanism')}")
    lines.append("")
    lines.append("Reasons:")
    for reason in payload.get("reasons", []):
        lines.append(f"- {reason}")
    lines.append("")
    lines.append("Candidate mechanisms:")
    for c in payload.get("candidate_mechanisms", []):
        lines.append(f"- {c.get('mechanism_id')}: passes={c.get('passes')}, fields={c.get('fields')}")
    lines.append("")
    lines.append("Hard stops:")
    for stop in payload.get("hard_stops", []):
        lines.append(f"- {stop}")
    return "\n".join(lines) + "\n"


def run_non_cashflow_mechanism_preflight(
    dataset_path: str | Path = DEFAULT_DATASET,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    dataset_path = Path(dataset_path)
    output_dir = Path(output_dir)
    df = pd.read_csv(dataset_path)
    inventory = build_field_inventory(df)
    coverage = build_coverage_preflight(df, inventory)
    direction = build_direction_preflight(df, inventory)
    decision = build_mechanism_selection_decision(inventory, coverage, direction)

    _write_json_md(output_dir, "field_inventory", inventory, inventory_to_markdown(inventory))
    _write_json_md(output_dir, "coverage_preflight", coverage, coverage_to_markdown(coverage))
    _write_json_md(output_dir, "direction_preflight", direction, direction_to_markdown(direction))
    _write_json_md(output_dir, "mechanism_selection_decision", decision, decision_to_markdown(decision))
    return {"inventory": inventory, "coverage": coverage, "direction": direction, "decision": decision}
