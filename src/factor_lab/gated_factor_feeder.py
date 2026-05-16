from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from factor_lab.pit_cashflow_closure_policy import evaluate_cashflow_closure
from factor_lab.pit_data_preflight import preflight_report_ready
from factor_lab.research_gate import evaluate_research_gate


@dataclass(frozen=True)
class GatedVariant:
    variant_id: str
    route_id: str
    expression: str
    required_features: tuple[str, ...]
    description: str

    def to_config(self, hypothesis: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "artifact_type": "gated_research_factor_config",
            "hypothesis_id": hypothesis["hypothesis_id"],
            "mechanism_id": hypothesis.get("mechanism_id", hypothesis["hypothesis_id"]),
            "variant_id": self.variant_id,
            "route_id": self.route_id,
            "description": self.description,
            "data_source": "tushare",
            "cache_dir": "artifacts/tushare_cache",
            "start_date": hypothesis.get("experiment_window", {}).get("start_date", "2020-01-01"),
            "end_date": hypothesis.get("experiment_window", {}).get("end_date", "2023-12-31"),
            "num_stocks": hypothesis.get("experiment_window", {}).get("num_stocks", 100),
            "portfolio_cost_bps_per_turnover": 20.0,
            "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
            "factors": [{"name": self.variant_id, "expression": self.expression}],
            "required_data_fields": list(self.required_features),
            "required_pit_features": list(self.required_features),
            "pit_requirements": hypothesis.get("pit_requirements", {}),
            "governance": {
                "research_gate": "allow_preflight",
                "max_variants": hypothesis.get("max_variants"),
                "stop_rules": hypothesis.get("stop_rules"),
                "dry_run_only_until_explicit_go": True,
            },
        }


VALUE_TRAP_VARIANTS: tuple[GatedVariant, ...] = (
    GatedVariant(
        variant_id="industry_relative_value_plus_cashflow_quality",
        route_id="value_trap_filter_quality_confirmation",
        expression="industry_relative_book_yield + operating_cashflow_to_profit",
        required_features=("industry_relative_book_yield", "operating_cashflow_to_profit"),
        description="Industry-relative value with cash conversion confirmation.",
    ),
    GatedVariant(
        variant_id="industry_relative_value_plus_low_leverage",
        route_id="value_trap_filter_quality_confirmation",
        expression="industry_relative_book_yield - debt_to_asset",
        required_features=("industry_relative_book_yield", "debt_to_asset"),
        description="Industry-relative value penalized by balance-sheet leverage.",
    ),
    GatedVariant(
        variant_id="industry_relative_value_plus_cashflow_low_leverage_no_profit_deterioration",
        route_id="value_trap_filter_quality_confirmation",
        expression="industry_relative_book_yield + operating_cashflow_to_profit - debt_to_asset + profit_yoy",
        required_features=("industry_relative_book_yield", "operating_cashflow_to_profit", "debt_to_asset", "profit_yoy"),
        description="Cheap but not distressed: cashflow quality, low leverage, and no profit deterioration.",
    ),
)


def _load_preflight_report(preflight_report: dict[str, Any] | None, preflight_path: str | Path | None) -> dict[str, Any] | None:
    if preflight_report is not None:
        return preflight_report
    if preflight_path is None:
        return None
    path = Path(preflight_path)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_gated_factor_configs(
    hypothesis: dict[str, Any],
    *,
    preflight_report: dict[str, Any] | None = None,
    preflight_path: str | Path | None = None,
    max_variants: int | None = None,
) -> dict[str, Any]:
    gate = evaluate_research_gate(hypothesis)
    if not gate.allowed:
        return {"decision": "blocked", "gate": gate.to_dict(), "configs": []}
    loaded_preflight = _load_preflight_report(preflight_report, preflight_path)
    preflight = preflight_report_ready(loaded_preflight or {})
    if not preflight["ready"]:
        return {"decision": "blocked", "gate": gate.to_dict(), "preflight": preflight, "configs": []}
    if hypothesis.get("hypothesis_id") != "value_trap_filter_quality_confirmation":
        return {"decision": "blocked", "gate": gate.to_dict(), "preflight": preflight, "configs": [], "reasons": ["unsupported_hypothesis_id"]}
    limit = min(int(hypothesis.get("max_variants", 3)), max_variants or 3, 3)
    configs = [variant.to_config(hypothesis) for variant in VALUE_TRAP_VARIANTS[:limit]]
    kept: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for config in configs:
        closure = evaluate_cashflow_closure(config)
        if closure.allowed:
            kept.append(config)
        else:
            blocked.append({"variant_id": config.get("variant_id"), **closure.to_dict()})
    decision = "dry_run_ready" if kept else "blocked"
    result = {"decision": decision, "gate": gate.to_dict(), "preflight": preflight, "configs": kept, "count": len(kept)}
    if blocked:
        result["cashflow_closure_blocked"] = blocked
    if not kept and blocked:
        result["reasons"] = ["cashflow_conditioning_closed"]
    return result


def write_gated_factor_configs(
    hypothesis: dict[str, Any],
    *,
    output_dir: str | Path,
    write: bool = False,
    preflight_report: dict[str, Any] | None = None,
    preflight_path: str | Path | None = None,
    max_variants: int | None = None,
    queue_db_path: str | Path | None = None,
) -> dict[str, Any]:
    # queue_db_path is intentionally unused: this helper never enqueues.
    _ = queue_db_path
    result = build_gated_factor_configs(hypothesis, preflight_report=preflight_report, preflight_path=preflight_path, max_variants=max_variants)
    out_dir = Path(output_dir)
    files: list[str] = []
    if write:
        out_dir.mkdir(parents=True, exist_ok=True)
        for old in out_dir.glob("*.json"):
            old.unlink()
    if write and result.get("configs"):
        for config in result["configs"]:
            path = out_dir / f"{config['variant_id']}.json"
            path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
            files.append(str(path))
    result["write"] = write
    result["files"] = files
    result["output_dir"] = str(out_dir)
    result["queue_written"] = False
    return result
