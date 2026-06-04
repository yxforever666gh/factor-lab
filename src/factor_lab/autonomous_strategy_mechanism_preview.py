from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_mechanism_researcher_preview_response(*, run_id: str, request_pack: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        {
            "route_id": "earnings_revision_valuation_repair_v2",
            "mechanism_family": "earnings_revision_valuation_repair",
            "economic_mechanism": "Cheap stocks only become investable when forward earnings expectations or post-announcement revisions improve, adding a catalyst distinct from static cheapness.",
            "required_fields": ["forecast_ann_date", "forecast_eps", "forecast_net_profit", "forecast_period_end", "actual_report_ann_date", "actual_net_profit", "date", "ticker", "industry", "forward_return_5d"],
            "data_status": "request_data",
            "cheap_screens": ["pit_alignment_screen", "revision_direction_screen", "cheap_plus_revision_forward_return_screen", "drawdown_proxy_screen"],
            "falsification_criteria": ["forecast/revision coverage below threshold", "positive revision among cheap names does not improve spread", "drawdown proxy remains below -0.35"],
        },
        {
            "route_id": "balance_sheet_improvement_recovery_v1",
            "mechanism_family": "balance_sheet_improvement_recovery",
            "economic_mechanism": "Value traps may recover only after leverage or cash conversion improves; the catalyst is balance-sheet repair rather than cheapness alone.",
            "required_fields": ["debt_to_asset", "debt_to_asset_delta", "operating_cashflow_to_profit", "roe", "date", "ticker", "industry", "forward_return_5d"],
            "data_status": "proxy_available_requires_review",
            "cheap_screens": ["balance_sheet_improvement_screen", "cashflow_quality_screen", "cheap_recovery_spread_screen", "drawdown_proxy_screen"],
            "falsification_criteria": ["debt/cashflow improvement does not reduce drawdown", "spread becomes non-positive after quality overlay", "usable universe shrinks below minimum"],
        },
        {
            "route_id": "industry_cycle_inflection_value_anchor_v1",
            "mechanism_family": "industry_cycle_inflection_with_value_anchor",
            "economic_mechanism": "Cheap stocks may work only when their industry cycle is inflecting upward; the catalyst is industry-level recovery, not individual valuation alone.",
            "required_fields": ["industry", "industry_return_60d", "industry_relative_pb", "industry_relative_earnings_yield", "date", "ticker", "forward_return_5d"],
            "data_status": "derivable_from_available_market_history",
            "cheap_screens": ["industry_cycle_inflection_screen", "industry_neutral_value_anchor_screen", "drawdown_proxy_screen"],
            "falsification_criteria": ["industry inflection does not improve cheap spread", "effect is explained solely by market beta", "drawdown proxy remains below -0.35"],
        },
    ]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_key": "factor_lab_mechanism_researcher",
        "decision_recommendation": "switch_mechanism_route",
        "reason_codes": ["prior_routes_stopped", "new_catalyst_required", "no_controlled_execution_without_new_screens"],
        "summary": "Proposed three catalyst-driven candidate routes distinct from static valuation cheapness. Two require data/proxy review; one is derivable from market/industry history.",
        "candidate_routes": candidates,
        "requested_actions": ["build_route_registry_v2", "run_field_resolution_for_candidates"],
        "forbidden_actions_observed": [],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
    }


def write_mechanism_researcher_preview_response(response: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    jp = out / "factor_lab_mechanism_researcher_response.json"
    mp = out / "factor_lab_mechanism_researcher_response.md"
    jp.write_text(json.dumps(response, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    lines = ["# Mechanism Researcher Preview Response", "", f"decision_recommendation: {response.get('decision_recommendation')}", f"summary: {response.get('summary')}", "", "## Candidate routes"]
    for route in response.get("candidate_routes") or []:
        lines.append(f"- {route.get('route_id')}: {route.get('mechanism_family')} ({route.get('data_status')})")
    mp.write_text("\n".join(lines).rstrip()+"\n", encoding="utf-8")
    return {"json": jp, "markdown": mp}
