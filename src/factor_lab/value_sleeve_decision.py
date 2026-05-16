
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.value_route_correlation_overlap import DEFAULT_ARTIFACT_DIR


def decide_value_sleeve(scorecard:dict[str,Any]|None, correlation:dict[str,Any]|None, sleeve:dict[str,Any]|None) -> dict[str,Any]:
    if not scorecard or not correlation or not sleeve:
        return {"decision":"hold_portfolio_expansion_pending_data_enrichment","primary_route":None,"confirmation_route":None,"low_weight_route":None,"reasons":["missing_diagnostics"],"recommended_next_action":"complete_missing_diagnostics"}
    routes=scorecard.get("routes", [])
    primary=max(routes, key=lambda r: r.get("base_spread_mean") or -999, default={}).get("route_id")
    by_role={r.get("recommended_role"):r.get("route_id") for r in routes}
    duplicate=correlation.get("decision") in {"high_duplicate_risk","shared_payoff_risk"}
    combos=[r for r in sleeve.get("combinations", []) if r.get("status")=="ok"]
    improves=any((r.get("spread_improvement_vs_quality") or 0)>0 and (r.get("spread_std_reduction_vs_quality") or 0)>0 for r in combos)
    reasons=[]
    if duplicate: reasons.append(f"correlation_overlap_decision={correlation.get('decision')}")
    if not improves: reasons.append("no_combination_improves_both_spread_and_stability_vs_quality_primary")
    if duplicate or not improves:
        decision="collapse_to_value_sleeve_with_primary_route"
        next_action="implement_value_sleeve_policy_after_user_go"
    else:
        decision="keep_separate_promoted_routes"
        next_action="keep_routes_separate_and_validate_combo_workflow_after_user_go"
    return {"schema_version":1,"decision":decision,"primary_route":primary or by_role.get("primary_candidate") or "value_quality_no_distress","confirmation_route":by_role.get("confirmation_candidate") or "value_momentum_confirmation","low_weight_route":by_role.get("low_weight_core_value_candidate") or "industry_relative_value","reasons":reasons,"recommended_next_action":next_action,"do_not_do":["restore_broad_daemon","increase_feeder_frequency","enqueue_more_single_route_followups"]}


def _read(path:Path)->dict[str,Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def build_value_sleeve_decision(*, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR)->dict[str,Any]:
    d=Path(artifact_dir)/"value_sleeve_validation"
    return decide_value_sleeve(_read(d/"route_scorecard.json"), _read(d/"route_correlation_overlap.json"), _read(d/"sleeve_portfolio_validation.json"))


def to_markdown(payload:dict[str,Any])->str:
    lines=["# Value Sleeve Decision","",f"Decision: {payload.get('decision')}",f"Primary route: {payload.get('primary_route')}",f"Confirmation route: {payload.get('confirmation_route')}",f"Low-weight route: {payload.get('low_weight_route')}",f"Recommended next action: {payload.get('recommended_next_action')}","","Reasons:"]
    lines += [f"- {r}" for r in payload.get("reasons", [])] or ["- none"]
    lines += ["","Do not do:"] + [f"- {x}" for x in payload.get("do_not_do", [])]
    return "\n".join(lines)+"\n"


def write_value_sleeve_decision(*, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR, output_dir: str|Path|None=None)->dict[str,Any]:
    artifact_dir=Path(artifact_dir); out=Path(output_dir) if output_dir else artifact_dir/"value_sleeve_validation"; out.mkdir(parents=True, exist_ok=True)
    payload=build_value_sleeve_decision(artifact_dir=artifact_dir)
    (out/"value_sleeve_decision.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out/"value_sleeve_decision.md").write_text(to_markdown(payload), encoding="utf-8")
    return payload
