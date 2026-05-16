from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_value_trap_field_fix"
REPAIRED_CONFIG_DIR = DEFAULT_OUTPUT_DIR / "repaired_configs"

TRANSFORM_FIELD_BY_SOURCE = {
    "operating_cashflow_to_profit": {
        "reverse": "reversed_operating_cashflow_to_profit_zscore_by_date_industry",
        "transform_only": "operating_cashflow_to_profit_zscore_by_date_industry",
        "keep": "operating_cashflow_to_profit_zscore_by_date_industry",
    },
    "debt_to_assets": {
        "reverse": "low_debt_to_assets_zscore_by_date_industry",
        "transform_only": "low_debt_to_assets_zscore_by_date_industry",
        "keep": "debt_to_assets_zscore_by_date_industry",
    },
    "netprofit_yoy": {
        "reverse": "reversed_netprofit_yoy_zscore_by_date_industry",
        "transform_only": "netprofit_yoy_zscore_by_date_industry",
        "keep": "netprofit_yoy_zscore_by_date_industry",
    },
    "tr_yoy": {
        "reverse": "reversed_tr_yoy_zscore_by_date_industry",
        "transform_only": "tr_yoy_zscore_by_date_industry",
        "keep": "tr_yoy_zscore_by_date_industry",
    },
}


def _load_decision(output_dir: Path) -> dict[str, Any]:
    return json.loads((output_dir / "field_decision.json").read_text(encoding="utf-8"))


def _eligible_transformed_fields(decision: dict[str, Any]) -> list[dict[str, str]]:
    rows = []
    for row in decision.get("fields", []):
        if not row.get("eligible_for_repaired_combo"):
            continue
        source = row["field"]
        action = row["action"]
        transformed = TRANSFORM_FIELD_BY_SOURCE.get(source, {}).get(action)
        if transformed:
            rows.append({"source_field": source, "action": action, "transformed_field": transformed})
    return rows


def build_repair_configs(decision: dict[str, Any], *, max_configs: int = 3) -> dict[str, Any]:
    eligible = _eligible_transformed_fields(decision)
    configs: list[dict[str, Any]] = []
    if not eligible:
        return {
            "schema_version": 1,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "no_configs_generated_no_fields_clear_gates",
            "configs": [],
            "eligible_transformed_fields": [],
        }
    # Prefer one conservative combined repair plus up to two single-field probes.
    selected = eligible[:3]
    combined_terms = ["industry_relative_book_yield", *[item["transformed_field"] for item in selected]]
    variants = [("repaired_standardized_combo", combined_terms)]
    for item in selected[: max(0, max_configs - 1)]:
        variants.append((f"repaired_{item['source_field']}_{item['action']}", ["industry_relative_book_yield", item["transformed_field"]]))
    for variant_id, terms in variants[:max_configs]:
        expression = " + ".join(terms)
        configs.append({
            "schema_version": 1,
            "artifact_type": "pit_value_trap_repaired_config",
            "hypothesis_id": "pit_value_trap_field_fix",
            "mechanism_id": "value_trap_filter_quality_confirmation",
            "variant_id": variant_id,
            "route_id": "value_trap_filter_quality_confirmation",
            "description": "Repaired PIT value-trap probe using standardized direction-adjusted fields only.",
            "data_source": "tushare",
            "cache_dir": "artifacts/tushare_cache",
            "start_date": "2020-01-01",
            "end_date": "2023-12-31",
            "num_stocks": 100,
            "portfolio_cost_bps_per_turnover": 20.0,
            "pit_value_trap_repair": True,
            "standardized_pit_features": True,
            "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
            "factors": [{"name": variant_id, "expression": expression}],
            "required_data_fields": terms,
            "required_pit_features": [item["source_field"] for item in selected],
            "pit_requirements": {
                "require_ann_date_asof": True,
                "prefer_f_ann_date": True,
                "forbid_end_date_only": True,
                "persist_source_ann_date": True,
                "persist_source_end_date": True,
            },
            "governance": {
                "research_gate": "allow_preflight",
                "max_variants": 3,
                "baseline_to_beat": "value_quality_no_distress",
                "baseline_spread_to_beat": 0.006225,
                "field_decision_source": "artifacts/pit_value_trap_field_fix/field_decision.json",
            },
        })
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": "generated_repaired_configs",
        "eligible_transformed_fields": eligible,
        "configs": configs,
    }


def manifest_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Value-Trap Repair Manifest", "", f"Generated: {payload.get('generated_at_utc')}", f"Decision: {payload.get('decision')}", ""]
    lines.append("## Eligible transformed fields")
    for item in payload.get("eligible_transformed_fields", []):
        lines.append(f"- {item['source_field']} -> {item['transformed_field']} ({item['action']})")
    if not payload.get("eligible_transformed_fields"):
        lines.append("- none")
    lines += ["", "## Configs"]
    for cfg in payload.get("configs", []):
        lines.append(f"- {cfg['variant_id']}: {cfg['factors'][0]['expression']}")
    if not payload.get("configs"):
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_repair_batch(output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir = Path(output_dir)
    REPAIRED_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    for old in REPAIRED_CONFIG_DIR.glob("*.json"):
        old.unlink()
    payload = build_repair_configs(_load_decision(output_dir))
    for cfg in payload.get("configs", []):
        path = REPAIRED_CONFIG_DIR / f"{cfg['variant_id']}.json"
        path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "repair_manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "repair_manifest.md").write_text(manifest_to_markdown(payload), encoding="utf-8")
    return payload
