
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACT_DIR = ROOT / "artifacts"
ROUTES = ["industry_relative_value", "value_momentum_confirmation", "value_quality_no_distress"]


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _first_result(path: Path) -> dict[str, Any] | None:
    payload = _read_json(path)
    if isinstance(payload, list) and payload:
        return payload[0]
    if isinstance(payload, dict):
        return payload
    return None


def _evidence_paths(artifact_dir: Path, route: str) -> dict[str, Path]:
    return {
        "base": artifact_dir / "value_route_bucket_aware" / "runs" / f"{route}_bucket_aware" / "bucket_aware_portfolio_results.json",
        "cost_20bps": artifact_dir / "value_route_followups" / "runs" / f"{route}__cost_sensitivity_20bps" / "bucket_aware_portfolio_results.json",
        "stricter_tail": artifact_dir / "value_route_followups" / "runs" / f"{route}__bucket_pair_stricter_tail" / "bucket_aware_portfolio_results.json",
    }


def _spread(row: dict[str, Any] | None) -> float | None:
    if not row:
        return None
    val = row.get("spread_mean")
    return None if val is None else float(val)


def _pass(row: dict[str, Any] | None) -> bool:
    return bool(row and row.get("pass_gate") is True)


def build_scorecard_rows(*, artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR, routes: list[str] | None = None) -> list[dict[str, Any]]:
    artifact_dir = Path(artifact_dir)
    policy = _read_json(artifact_dir / "controlled_route_policy.json") or {}
    decisions = policy.get("routes", {}) if isinstance(policy, dict) else {}
    rows=[]
    for route in routes or ROUTES:
        paths=_evidence_paths(artifact_dir, route)
        base=_first_result(paths["base"])
        cost=_first_result(paths["cost_20bps"])
        tail=_first_result(paths["stricter_tail"])
        base_spread=_spread(base)
        cost_spread=_spread(cost)
        tail_spread=_spread(tail)
        missing=[name for name,row in [("base",base),("cost_20bps",cost),("stricter_tail",tail)] if row is None]
        tail_deg=None
        if base_spread not in (None,0) and tail_spread is not None:
            tail_deg=round((base_spread-tail_spread)/base_spread,6)
        pass_all=bool(not missing and _pass(base) and _pass(cost) and _pass(tail))
        rows.append({
            "route_id": route,
            "base_spread_mean": base_spread,
            "cost_20bps_spread_mean": cost_spread,
            "stricter_tail_spread_mean": tail_spread,
            "tail_degradation_ratio": tail_deg,
            "observations": int((base or {}).get("observations") or 0),
            "pass_gate_all_available_evidence": pass_all,
            "policy_decision": (decisions.get(route) or {}).get("decision"),
            "missing_evidence": missing,
            "recommended_role": "hold",
            "preliminary_weight": 0.0,
        })
    complete=[r for r in rows if r["pass_gate_all_available_evidence"] and r["policy_decision"]=="promote"]
    complete.sort(key=lambda r: ((r["base_spread_mean"] or -999), (r["cost_20bps_spread_mean"] or -999)), reverse=True)
    roles=[("primary_candidate",0.50),("confirmation_candidate",0.30),("low_weight_core_value_candidate",0.20)]
    role_by_route={r["route_id"]: roles[i] for i,r in enumerate(complete[:3])}
    for r in rows:
        if r["missing_evidence"]:
            r["recommended_role"]="incomplete_evidence"
            r["preliminary_weight"]=0.0
        elif r["route_id"] in role_by_route:
            role, weight=role_by_route[r["route_id"]]
            r["recommended_role"]=role
            r["preliminary_weight"]=weight
        elif r["pass_gate_all_available_evidence"]:
            r["recommended_role"]="supporting_candidate"
            r["preliminary_weight"]=0.0
    return rows


def build_route_scorecard(*, artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR) -> dict[str, Any]:
    rows=build_scorecard_rows(artifact_dir=artifact_dir)
    return {"schema_version":1,"generated_at_utc":datetime.now(timezone.utc).isoformat(),"routes":rows}


def to_markdown(payload: dict[str, Any]) -> str:
    lines=["# Value Route Scorecard","",f"Generated: {payload.get('generated_at_utc')}","","| Route | Base | Cost 20bps | Strict tail | Tail degradation | Policy | Role | Weight |","|---|---:|---:|---:|---:|---|---|---:|"]
    for r in payload.get("routes",[]):
        lines.append(f"| {r['route_id']} | {r.get('base_spread_mean')} | {r.get('cost_20bps_spread_mean')} | {r.get('stricter_tail_spread_mean')} | {r.get('tail_degradation_ratio')} | {r.get('policy_decision')} | {r.get('recommended_role')} | {r.get('preliminary_weight')} |")
    return "\n".join(lines)+"\n"


def write_route_scorecard(*, artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR, output_dir: str | Path | None = None) -> dict[str, Any]:
    artifact_dir=Path(artifact_dir)
    out=Path(output_dir) if output_dir else artifact_dir/"value_sleeve_validation"
    out.mkdir(parents=True, exist_ok=True)
    payload=build_route_scorecard(artifact_dir=artifact_dir)
    (out/"route_scorecard.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out/"route_scorecard.md").write_text(to_markdown(payload), encoding="utf-8")
    return payload
