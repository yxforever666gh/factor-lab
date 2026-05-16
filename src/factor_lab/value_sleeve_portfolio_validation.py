
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.bucket_aware_portfolio import evaluate_bucket_pair_portfolio
from factor_lab.value_route_correlation_overlap import DEFAULT_ARTIFACT_DIR, reconstruct_route_frame

COMBINATIONS={
    "quality_plus_momentum_equal_weight":{"value_quality_no_distress":0.5,"value_momentum_confirmation":0.5},
    "quality_plus_industry_value_equal_weight":{"value_quality_no_distress":0.5,"industry_relative_value":0.5},
    "three_route_equal_weight":{"industry_relative_value":1/3,"value_momentum_confirmation":1/3,"value_quality_no_distress":1/3},
}


def zscore_by_date(frame: pd.DataFrame, *, value_col: str="factor_value") -> pd.DataFrame:
    out=frame.copy()
    def z(s: pd.Series) -> pd.Series:
        std=s.std(ddof=0)
        if std == 0 or pd.isna(std):
            return s*0.0
        return (s-s.mean())/std
    out["normalized_factor_value"]=out.groupby("date")[value_col].transform(z)
    return out


def normalize_weights(weights: dict[str,float]) -> dict[str,float]:
    pos={k:float(v) for k,v in weights.items() if float(v)>0}
    total=sum(pos.values())
    return {k:v/total for k,v in pos.items()} if total else {}


def compose_sleeve_signal(frames: dict[str,pd.DataFrame], weights: dict[str,float]) -> tuple[pd.DataFrame, dict[str,Any]]:
    weights=normalize_weights(weights)
    merged=None
    base_counts={r:int(len(frames[r])) for r in weights if r in frames}
    for route,w in weights.items():
        f=zscore_by_date(frames[route])[["date","ticker","forward_return_5d","normalized_factor_value"]].copy()
        f=f.rename(columns={"normalized_factor_value":f"score_{route}"})
        merged=f if merged is None else merged.merge(f, on=["date","ticker","forward_return_5d"], how="inner")
    if merged is None or merged.empty:
        return pd.DataFrame(), {"status":"blocked","reason":"no_common_rows","common_coverage_ratio":0.0,"base_counts":base_counts}
    merged["factor_value"]=0.0
    for route,w in weights.items():
        merged["factor_value"] += merged[f"score_{route}"]*w
    min_base=min(base_counts.values()) if base_counts else 0
    coverage=float(len(merged)/min_base) if min_base else 0.0
    return merged[["date","ticker","forward_return_5d","factor_value"]].copy(), {"status":"ok","weights":weights,"rows":int(len(merged)),"common_coverage_ratio":round(coverage,6),"base_counts":base_counts}


def _load_scorecard_weights(artifact_dir: Path) -> dict[str,float]:
    path=artifact_dir/"value_sleeve_validation"/"route_scorecard.json"
    if not path.exists():
        return {"value_quality_no_distress":0.5,"value_momentum_confirmation":0.3,"industry_relative_value":0.2}
    data=json.loads(path.read_text(encoding="utf-8"))
    return {r["route_id"]:float(r.get("preliminary_weight") or 0) for r in data.get("routes", [])}


def build_sleeve_portfolio_validation(*, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR) -> dict[str,Any]:
    artifact_dir=Path(artifact_dir)
    routes=["industry_relative_value","value_momentum_confirmation","value_quality_no_distress"]
    frames={}; reconstruction={}
    for r in routes:
        frames[r], reconstruction[r]=reconstruct_route_frame(r, artifact_dir=artifact_dir)
    combos=dict(COMBINATIONS)
    combos["scorecard_weighted_value_sleeve"]=_load_scorecard_weights(artifact_dir)
    results=[]
    primary=frames.get("value_quality_no_distress", pd.DataFrame())
    primary_res=None
    if not primary.empty:
        primary_res=evaluate_bucket_pair_portfolio(primary, long_quantile=3, short_quantile=0).to_dict()
    for name,weights in combos.items():
        if any(reconstruction.get(r,{}).get("status")!="ok" for r in weights):
            results.append({"combination_id":name,"status":"blocked","reason":"route_reconstruction_blocked","weights":weights}); continue
        sleeve, meta=compose_sleeve_signal(frames, weights)
        if meta.get("status")!="ok":
            results.append({"combination_id":name, **meta}); continue
        base=evaluate_bucket_pair_portfolio(sleeve, long_quantile=3, short_quantile=0).to_dict()
        tail=evaluate_bucket_pair_portfolio(sleeve, long_quantile=4, short_quantile=0).to_dict()
        improvement=None
        stability=None
        if primary_res:
            improvement=round(float(base["spread_mean"])-float(primary_res["spread_mean"]),6)
            stability=round(float(primary_res["spread_std"])-float(base["spread_std"]),6)
        results.append({"combination_id":name,"status":"ok","weights":meta["weights"],"common_coverage_ratio":meta["common_coverage_ratio"],"rows":meta["rows"],"bucket_pair_q3_q0":base,"strict_tail_q4_q0":tail,"spread_improvement_vs_quality":improvement,"spread_std_reduction_vs_quality":stability})
    best=max([r for r in results if r.get("status")=="ok"], key=lambda r: r["bucket_pair_q3_q0"]["spread_mean"], default=None)
    return {"schema_version":1,"reconstruction":reconstruction,"primary_route_reference":primary_res,"combinations":results,"best_combination_id": best.get("combination_id") if best else None}


def to_markdown(payload:dict[str,Any])->str:
    lines=["# Value Sleeve Portfolio Validation","",f"Best combination: {payload.get('best_combination_id')}","","| Combination | Q3-Q0 spread | Q3-Q0 std | Q4-Q0 spread | Coverage | Improve vs quality | Std reduction |","|---|---:|---:|---:|---:|---:|---:|"]
    for r in payload.get("combinations",[]):
        if r.get("status")!="ok":
            lines.append(f"| {r.get('combination_id')} | blocked | | | | | |")
            continue
        b=r["bucket_pair_q3_q0"]; t=r["strict_tail_q4_q0"]
        lines.append(f"| {r['combination_id']} | {b.get('spread_mean')} | {b.get('spread_std')} | {t.get('spread_mean')} | {r.get('common_coverage_ratio')} | {r.get('spread_improvement_vs_quality')} | {r.get('spread_std_reduction_vs_quality')} |")
    return "\n".join(lines)+"\n"


def write_sleeve_portfolio_validation(*, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR, output_dir: str|Path|None=None)->dict[str,Any]:
    artifact_dir=Path(artifact_dir); out=Path(output_dir) if output_dir else artifact_dir/"value_sleeve_validation"; out.mkdir(parents=True, exist_ok=True)
    payload=build_sleeve_portfolio_validation(artifact_dir=artifact_dir)
    (out/"sleeve_portfolio_validation.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out/"sleeve_portfolio_validation.md").write_text(to_markdown(payload), encoding="utf-8")
    return payload
