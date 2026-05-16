
from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACT_DIR = ROOT / "artifacts"
ROUTE_EXPRESSIONS={
    "industry_relative_value": ("industry_relative_book_yield", ["industry_relative_book_yield"]),
    "value_quality_no_distress": ("industry_relative_book_yield + roe", ["industry_relative_book_yield","roe"]),
    "value_momentum_confirmation": ("industry_relative_book_yield + momentum_60_skip_5", ["industry_relative_book_yield","momentum_60_skip_5"]),
}
ROUTE_BUCKETS={
    "industry_relative_value": {"quantiles":5,"long_quantile":3,"short_quantile":0},
    "value_quality_no_distress": {"quantiles":5,"long_quantile":3,"short_quantile":0},
    "value_momentum_confirmation": {"quantiles":5,"long_quantile":3,"short_quantile":1},
}


def reconstruct_route_frame(route_id: str, *, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR, dataset_path: str|Path|None=None) -> tuple[pd.DataFrame, dict[str, Any]]:
    if route_id not in ROUTE_EXPRESSIONS:
        return pd.DataFrame(), {"status":"blocked","reason":"unknown_route"}
    expr, cols=ROUTE_EXPRESSIONS[route_id]
    path=Path(dataset_path) if dataset_path else Path(artifact_dir)/"value_route_bucket_aware"/"runs"/f"{route_id}_bucket_aware"/"dataset.csv"
    if not path.exists():
        return pd.DataFrame(), {"status":"blocked","reason":"missing_dataset","path":str(path)}
    df=pd.read_csv(path)
    required={"date","ticker","forward_return_5d",*cols}
    missing=sorted(required-set(df.columns))
    if missing:
        return pd.DataFrame(), {"status":"blocked","reason":"missing_required_columns","missing_columns":missing}
    out=df[["date","ticker","forward_return_5d",*cols]].copy()
    for c in ["forward_return_5d",*cols]:
        out[c]=pd.to_numeric(out[c], errors="coerce")
    if route_id=="industry_relative_value":
        out["factor_value"]=out["industry_relative_book_yield"]
    elif route_id=="value_quality_no_distress":
        out["factor_value"]=out["industry_relative_book_yield"] + out["roe"]
    else:
        out["factor_value"]=out["industry_relative_book_yield"] + out["momentum_60_skip_5"]
    out=out.dropna(subset=["date","ticker","forward_return_5d","factor_value"])
    out=out[["date","ticker","forward_return_5d","factor_value"]].copy()
    out["route_id"]=route_id
    return out, {"status":"ok","route_id":route_id,"expression":expr,"rows":int(len(out)),"dates":int(out["date"].nunique())}


def assign_buckets(frame: pd.DataFrame, *, quantiles:int=5) -> pd.DataFrame:
    parts=[]
    for date,g in frame.groupby("date", sort=True):
        if len(g)<quantiles or g["factor_value"].nunique()<quantiles: continue
        gg=g.copy()
        gg["bucket"]=pd.qcut(gg["factor_value"].rank(method="first"), quantiles, labels=False, duplicates="drop")
        parts.append(gg)
    return pd.concat(parts, ignore_index=True) if parts else frame.assign(bucket=pd.Series(dtype="float"))


def daily_spreads(frame: pd.DataFrame, cfg: dict[str,int]) -> pd.Series:
    b=assign_buckets(frame, quantiles=cfg["quantiles"])
    vals={}
    for date,g in b.groupby("date"):
        if cfg["long_quantile"] not in set(g["bucket"]) or cfg["short_quantile"] not in set(g["bucket"]): continue
        vals[date]=float(g.loc[g.bucket==cfg["long_quantile"],"forward_return_5d"].mean()-g.loc[g.bucket==cfg["short_quantile"],"forward_return_5d"].mean())
    return pd.Series(vals, name="spread")


def _jaccard(a:set,b:set)->float:
    return float(len(a&b)/len(a|b)) if (a or b) else 0.0


def pair_metrics(route_a:str, frame_a:pd.DataFrame, route_b:str, frame_b:pd.DataFrame) -> dict[str, Any]:
    m=frame_a.merge(frame_b, on=["date","ticker"], suffixes=("_a","_b"))
    factor_corr=float(m["factor_value_a"].corr(m["factor_value_b"], method="spearman")) if len(m)>1 else None
    sa=daily_spreads(frame_a, ROUTE_BUCKETS[route_a]); sb=daily_spreads(frame_b, ROUTE_BUCKETS[route_b])
    sj=pd.concat([sa.rename("a"), sb.rename("b")], axis=1, join="inner").dropna()
    spread_corr=float(sj["a"].corr(sj["b"])) if len(sj)>1 else None
    ba=assign_buckets(frame_a, quantiles=ROUTE_BUCKETS[route_a]["quantiles"])
    bb=assign_buckets(frame_b, quantiles=ROUTE_BUCKETS[route_b]["quantiles"])
    common=sorted(set(ba.date)&set(bb.date))
    long_ovs=[]; short_ovs=[]; selected_ovs=[]
    for d in common:
        ga=ba[ba.date==d]; gb=bb[bb.date==d]
        la=set(ga.loc[ga.bucket==ROUTE_BUCKETS[route_a]["long_quantile"],"ticker"]); lb=set(gb.loc[gb.bucket==ROUTE_BUCKETS[route_b]["long_quantile"],"ticker"])
        sa_=set(ga.loc[ga.bucket==ROUTE_BUCKETS[route_a]["short_quantile"],"ticker"]); sb_=set(gb.loc[gb.bucket==ROUTE_BUCKETS[route_b]["short_quantile"],"ticker"])
        long_ovs.append(_jaccard(la,lb)); short_ovs.append(_jaccard(sa_,sb_)); selected_ovs.append(_jaccard(la|sa_, lb|sb_))
    mean=lambda xs: round(float(sum(xs)/len(xs)),6) if xs else None
    return {"route_a":route_a,"route_b":route_b,"factor_score_spearman_corr":None if factor_corr is None or pd.isna(factor_corr) else round(factor_corr,6),"daily_spread_corr":None if spread_corr is None or pd.isna(spread_corr) else round(spread_corr,6),"long_bucket_overlap_mean":mean(long_ovs),"short_bucket_overlap_mean":mean(short_ovs),"selected_bucket_overlap_mean":mean(selected_ovs),"common_observations":int(len(m)),"common_dates":int(len(common))}


def build_correlation_overlap(*, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR, routes:list[str]|None=None) -> dict[str, Any]:
    routes=routes or list(ROUTE_EXPRESSIONS)
    frames={}; reconstruction={}
    for r in routes:
        frames[r], reconstruction[r]=reconstruct_route_frame(r, artifact_dir=artifact_dir)
    pairs=[pair_metrics(a,frames[a],b,frames[b]) for a,b in combinations(routes,2) if reconstruction[a].get("status")=="ok" and reconstruction[b].get("status")=="ok"]
    high=any((p.get("factor_score_spearman_corr") or 0)>=0.85 or (p.get("selected_bucket_overlap_mean") or 0)>=0.70 for p in pairs)
    spread_high=sum(1 for p in pairs if (p.get("daily_spread_corr") or 0)>=0.70)
    if high: decision="high_duplicate_risk"
    elif pairs and spread_high >= max(1, len(pairs)//2+1): decision="shared_payoff_risk"
    elif pairs: decision="complementarity_supported"
    else: decision="insufficient_reconstruction"
    return {"schema_version":1,"reconstruction":reconstruction,"pairs":pairs,"decision":decision}


def to_markdown(payload:dict[str,Any])->str:
    lines=["# Value Route Correlation / Overlap","",f"Decision: {payload.get('decision')}","","| Pair | Factor corr | Daily spread corr | Long overlap | Short overlap | Selected overlap | Common dates |","|---|---:|---:|---:|---:|---:|---:|"]
    for p in payload.get("pairs",[]):
        lines.append(f"| {p['route_a']} / {p['route_b']} | {p.get('factor_score_spearman_corr')} | {p.get('daily_spread_corr')} | {p.get('long_bucket_overlap_mean')} | {p.get('short_bucket_overlap_mean')} | {p.get('selected_bucket_overlap_mean')} | {p.get('common_dates')} |")
    return "\n".join(lines)+"\n"


def write_correlation_overlap(*, artifact_dir: str|Path=DEFAULT_ARTIFACT_DIR, output_dir: str|Path|None=None)->dict[str,Any]:
    artifact_dir=Path(artifact_dir); out=Path(output_dir) if output_dir else artifact_dir/"value_sleeve_validation"; out.mkdir(parents=True, exist_ok=True)
    payload=build_correlation_overlap(artifact_dir=artifact_dir)
    (out/"route_correlation_overlap.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out/"route_correlation_overlap.md").write_text(to_markdown(payload), encoding="utf-8")
    return payload
