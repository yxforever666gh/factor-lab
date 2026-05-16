from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUN_DIR = ROOT / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_value_trap_attribution"
DEFAULT_FIELDS = [
    "operating_cashflow_to_profit",
    "debt_to_assets",
    "netprofit_yoy",
    "tr_yoy",
]
DEFAULT_BASELINE_FIELD = "industry_relative_book_yield"
DEFAULT_RETURN_FIELD = "forward_return_5d"


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _rank_ic_by_date(df: pd.DataFrame, field: str, return_field: str = DEFAULT_RETURN_FIELD) -> pd.Series:
    if field not in df.columns or return_field not in df.columns:
        return pd.Series(dtype="float64")
    work = df[["date", field, return_field]].copy()
    work[field] = _numeric(work[field])
    work[return_field] = _numeric(work[return_field])
    work = work.dropna(subset=[field, return_field])
    if work.empty:
        return pd.Series(dtype="float64")

    def _corr(group: pd.DataFrame) -> float | None:
        if group[field].nunique(dropna=True) < 2 or group[return_field].nunique(dropna=True) < 2:
            return None
        # Avoid scipy dependency: Spearman is Pearson correlation of ranks.
        x = group[field].rank(method="average")
        y = group[return_field].rank(method="average")
        return float(x.corr(y))

    values: list[float] = []
    for _, group in work.groupby("date"):
        val = _corr(group)
        if val is not None and not pd.isna(val):
            values.append(val)
    return pd.Series(values, dtype="float64")


def _rank_ic_summary(df: pd.DataFrame, field: str, return_field: str = DEFAULT_RETURN_FIELD) -> dict[str, Any]:
    series = _rank_ic_by_date(df, field, return_field)
    if series.empty:
        return {"field": field, "observations": 0, "rank_ic_mean": None, "rank_ic_std": None, "rank_ic_ir": None}
    std = float(series.std(ddof=1)) if len(series) > 1 else 0.0
    return {
        "field": field,
        "observations": int(len(series)),
        "rank_ic_mean": float(series.mean()),
        "rank_ic_std": std,
        "rank_ic_ir": None if std == 0 else float(series.mean() / std),
    }


def build_field_coverage_report(
    df: pd.DataFrame,
    fields: list[str] | None = None,
    *,
    final_fields: list[str] | None = None,
) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    final_fields = final_fields or [DEFAULT_BASELINE_FIELD, *fields]
    rows: list[dict[str, Any]] = []
    total = len(df)
    for field in fields:
        if field not in df.columns:
            rows.append({
                "field": field,
                "exists": False,
                "coverage": 0.0,
                "non_null_rows": 0,
                "coverage_by_year": {},
                "lowest_industry_coverage": [],
            })
            continue
        numeric = _numeric(df[field])
        by_year = {}
        if "date" in df.columns:
            years = pd.to_datetime(df["date"], errors="coerce").dt.year
            for year, group_idx in df.groupby(years).groups.items():
                if pd.isna(year):
                    continue
                mask = df.index.isin(group_idx)
                denom = int(mask.sum())
                by_year[str(int(year))] = None if denom == 0 else float(numeric[mask].notna().mean())
        lowest_industry: list[dict[str, Any]] = []
        if "industry" in df.columns:
            industry_cov = numeric.notna().groupby(df["industry"].fillna("UNKNOWN")).mean().sort_values().head(5)
            lowest_industry = [{"industry": str(idx), "coverage": float(val)} for idx, val in industry_cov.items()]
        rows.append({
            "field": field,
            "exists": True,
            "coverage": 0.0 if total == 0 else float(numeric.notna().mean()),
            "non_null_rows": int(numeric.notna().sum()),
            "coverage_by_year": by_year,
            "lowest_industry_coverage": lowest_industry,
        })
    present_final = [f for f in final_fields if f in df.columns]
    final_coverage = 0.0
    if present_final and total:
        final_coverage = float(df[present_final].apply(_numeric).notna().all(axis=1).mean())
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(total),
        "fields": rows,
        "final_expression_fields": final_fields,
        "final_expression_coverage": final_coverage,
        "coverage_thresholds": {"direct_combo_minimum": 0.30, "preferred_main_combo_minimum": 0.60},
    }


def build_distribution_report(df: pd.DataFrame, fields: list[str] | None = None) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows = []
    quantiles = [0.0, 0.01, 0.05, 0.5, 0.95, 0.99, 1.0]
    for field in fields:
        if field not in df.columns:
            rows.append({"field": field, "exists": False})
            continue
        s = _numeric(df[field]).dropna()
        if s.empty:
            rows.append({"field": field, "exists": True, "non_null_rows": 0})
            continue
        q = s.quantile(quantiles).to_dict()
        rows.append({
            "field": field,
            "exists": True,
            "non_null_rows": int(len(s)),
            "min": float(q[0.0]),
            "p01": float(q[0.01]),
            "p05": float(q[0.05]),
            "median": float(q[0.5]),
            "p95": float(q[0.95]),
            "p99": float(q[0.99]),
            "max": float(q[1.0]),
        })
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def build_single_field_ic_report(
    df: pd.DataFrame,
    fields: list[str] | None = None,
    *,
    baseline_field: str = DEFAULT_BASELINE_FIELD,
    return_field: str = DEFAULT_RETURN_FIELD,
) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows = []
    for field in [baseline_field, *fields]:
        row = _rank_ic_summary(df, field, return_field)
        row["reverse_rank_ic_mean"] = None if row["rank_ic_mean"] is None else -float(row["rank_ic_mean"])
        if row["rank_ic_mean"] is None:
            row["direction_decision"] = "missing_or_unusable"
        elif row["rank_ic_mean"] < -0.01:
            row["direction_decision"] = "negative_as_written"
        elif row["rank_ic_mean"] < 0.005:
            row["direction_decision"] = "weak_or_noisy"
        else:
            row["direction_decision"] = "positive_but_validate_robustness"
        rows.append(row)
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def _bucket_spread_for_field(
    df: pd.DataFrame,
    field: str,
    return_field: str = DEFAULT_RETURN_FIELD,
    quantiles: int = 5,
) -> dict[str, Any]:
    if field not in df.columns or return_field not in df.columns:
        return {"field": field, "observations": 0, "bucket_means": {}, "top_bottom_spread_mean": None, "bottom_top_spread_mean": None}
    work = df[["date", field, return_field]].copy()
    work[field] = _numeric(work[field])
    work[return_field] = _numeric(work[return_field])
    work = work.dropna(subset=[field, return_field])
    if work.empty:
        return {"field": field, "observations": 0, "bucket_means": {}, "top_bottom_spread_mean": None, "bottom_top_spread_mean": None}

    bucket_rows = []
    for _, group in work.groupby("date"):
        if len(group) < quantiles or group[field].nunique(dropna=True) < quantiles:
            continue
        buckets = pd.qcut(group[field].rank(method="first"), quantiles, labels=False, duplicates="drop")
        tmp = group.assign(bucket=buckets)
        means = tmp.groupby("bucket")[return_field].mean()
        if 0 in means.index and (quantiles - 1) in means.index:
            bucket_rows.append({"spread": float(means.loc[quantiles - 1] - means.loc[0])})
    bucket_means = {}
    if not work.empty:
        try:
            buckets = work.groupby("date", group_keys=False)[field].transform(lambda s: pd.qcut(s.rank(method="first"), quantiles, labels=False, duplicates="drop"))
            tmp = work.assign(bucket=buckets)
            bucket_means = {str(int(k)): float(v) for k, v in tmp.groupby("bucket")[return_field].mean().dropna().items()}
        except ValueError:
            bucket_means = {}
    spreads = pd.Series([r["spread"] for r in bucket_rows], dtype="float64")
    spread = None if spreads.empty else float(spreads.mean())
    return {
        "field": field,
        "observations": int(len(work)),
        "bucket_means": bucket_means,
        "top_bottom_spread_mean": spread,
        "bottom_top_spread_mean": None if spread is None else -spread,
    }


def build_single_field_bucket_report(df: pd.DataFrame, fields: list[str] | None = None, *, baseline_field: str = DEFAULT_BASELINE_FIELD) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows = [_bucket_spread_for_field(df, field) for field in [baseline_field, *fields]]
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def _winsorized(s: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    s = _numeric(s)
    if s.dropna().empty:
        return s
    lo, hi = s.quantile([lower, upper])
    return s.clip(lo, hi)


def _zscore(s: pd.Series) -> pd.Series:
    s = _numeric(s)
    std = s.std(ddof=0)
    if not std or pd.isna(std):
        return s * 0
    return (s - s.mean()) / std


def _date_industry_zscore(df: pd.DataFrame, field: str) -> pd.Series:
    s = _numeric(df[field])
    if "industry" not in df.columns:
        return df.groupby("date")[field].transform(_zscore)
    return s.groupby([df["date"], df["industry"].fillna("UNKNOWN")]).transform(_zscore)


def build_scaling_report(df: pd.DataFrame, fields: list[str] | None = None, *, return_field: str = DEFAULT_RETURN_FIELD) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    work = df.copy()
    rows = []
    for field in fields:
        if field not in work.columns:
            rows.append({"field": field, "exists": False})
            continue
        variants = {
            "raw": _numeric(work[field]),
            "winsorized_1_99": _winsorized(work[field]),
            "zscore_global": _zscore(work[field]),
            "zscore_date_industry": _date_industry_zscore(work, field),
        }
        variant_rows = []
        for name, values in variants.items():
            temp = work[["date", return_field]].copy()
            temp[f"__{field}"] = values
            summary = _rank_ic_summary(temp.rename(columns={f"__{field}": field}), field, return_field)
            variant_rows.append({"variant": name, **summary})
        rows.append({"field": field, "exists": True, "variants": variant_rows})
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def build_incremental_contribution_report(
    df: pd.DataFrame,
    fields: list[str] | None = None,
    *,
    baseline_field: str = DEFAULT_BASELINE_FIELD,
    return_field: str = DEFAULT_RETURN_FIELD,
) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows = []
    work = df.copy()
    baseline_summary = _rank_ic_summary(work, baseline_field, return_field)
    baseline_ic = baseline_summary.get("rank_ic_mean")
    for field in fields:
        if baseline_field not in work.columns or field not in work.columns:
            rows.append({"field": field, "exists": False})
            continue
        combo_field = f"__baseline_plus_{field}"
        # Conservative diagnostic: date+industry z-score both sides before adding.
        baseline_z = _date_industry_zscore(work, baseline_field)
        field_z = _date_industry_zscore(work, field)
        work[combo_field] = baseline_z + field_z
        combo_summary = _rank_ic_summary(work.rename(columns={combo_field: combo_field}), combo_field, return_field)
        combo_ic = combo_summary.get("rank_ic_mean")
        rows.append({
            "field": field,
            "exists": True,
            "baseline_field": baseline_field,
            "baseline_rank_ic_mean": baseline_ic,
            "combo_rank_ic_mean": combo_ic,
            "incremental_rank_ic": None if baseline_ic is None or combo_ic is None else float(combo_ic - baseline_ic),
            "combo_observations": combo_summary.get("observations"),
        })
        work.drop(columns=[combo_field], inplace=True)
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "baseline": baseline_summary, "fields": rows}


def build_missing_value_treatment_report(
    df: pd.DataFrame,
    fields: list[str] | None = None,
    *,
    baseline_field: str = DEFAULT_BASELINE_FIELD,
    return_field: str = DEFAULT_RETURN_FIELD,
) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows: list[dict[str, Any]] = []
    base = df.copy()
    for field in fields:
        if field not in base.columns:
            rows.append({"field": field, "exists": False})
            continue
        variants: list[dict[str, Any]] = []

        variants.append({"variant": "drop_missing", **_rank_ic_summary(base, field, return_field)})

        tmp = base.copy()
        s = _numeric(tmp[field])
        tmp[field] = s.fillna(s.median())
        variants.append({"variant": "global_median_fill", **_rank_ic_summary(tmp, field, return_field)})

        tmp = base.copy()
        s = _numeric(tmp[field])
        keys = [tmp["date"]]
        if "industry" in tmp.columns:
            keys.append(tmp["industry"].fillna("UNKNOWN"))
        med = s.groupby(keys).transform("median")
        tmp[field] = s.fillna(med).fillna(s.median())
        variants.append({"variant": "date_industry_median_fill", **_rank_ic_summary(tmp, field, return_field)})

        tmp = base.copy()
        flag = f"__{field}_missing_flag"
        tmp[flag] = _numeric(tmp[field]).isna().astype(float)
        tmp = tmp.drop(columns=[field]).rename(columns={flag: field})
        variants.append({"variant": "missing_flag_only", **_rank_ic_summary(tmp, field, return_field)})

        rows.append({"field": field, "exists": True, "variants": variants})
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}



def build_field_correlation_report(df: pd.DataFrame, fields: list[str] | None = None, *, baseline_field: str = DEFAULT_BASELINE_FIELD) -> dict[str, Any]:
    fields = [baseline_field, *(fields or DEFAULT_FIELDS)]
    available = [f for f in fields if f in df.columns]
    if not available:
        matrix = {}
    else:
        numeric = df[available].apply(_numeric)
        ranked = numeric.rank(method="average")
        corr = ranked.corr()
        matrix = {idx: {col: (None if pd.isna(val) else float(val)) for col, val in row.items()} for idx, row in corr.to_dict(orient="index").items()}
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": available, "spearman_correlation": matrix}


def build_final_decision(*, coverage: dict[str, Any], ic: dict[str, Any], scaling: dict[str, Any], incremental: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    hard_stops: list[str] = []
    field_actions: dict[str, str] = {}
    for row in coverage.get("fields", []):
        cov = float(row.get("coverage") or 0.0)
        field = row.get("field")
        if cov < 0.30:
            hard_stops.append(f"{field}: coverage_below_30pct")
            field_actions[field] = "do_not_use_in_combo_until_coverage_fixed"
        elif cov < 0.60:
            reasons.append(f"{field}: coverage_below_preferred_60pct")
            field_actions.setdefault(field, "cheap_screen_only")
    for row in ic.get("fields", []):
        field = row.get("field")
        if field == DEFAULT_BASELINE_FIELD:
            continue
        mean = row.get("rank_ic_mean")
        if mean is not None and float(mean) < -0.01:
            hard_stops.append(f"{field}: negative_single_field_ic")
            field_actions[field] = "test_reverse_or_drop"
        elif mean is not None and float(mean) < 0.005:
            reasons.append(f"{field}: weak_single_field_ic")
            field_actions.setdefault(field, "monitor_or_drop")
    for row in incremental.get("fields", []):
        field = row.get("field")
        inc = row.get("incremental_rank_ic")
        if inc is not None and float(inc) < 0:
            reasons.append(f"{field}: negative_incremental_ic_vs_baseline")
            field_actions.setdefault(field, "do_not_add_to_baseline_as_is")
    decision = "stop_value_trap_combo_line_pending_attribution_fix" if hard_stops else "allow_at_most_three_standardized_rebuilds"
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "hard_stops": hard_stops,
        "reasons": reasons,
        "field_actions": field_actions,
        "next_step": "fix coverage/direction/scaling first; do not run additional value-trap variants until the hard stops are cleared",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def decision_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# PIT Value-Trap Attribution Decision",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Decision: {payload.get('decision')}",
        "",
        "## Hard stops",
    ]
    hard_stops = payload.get("hard_stops") or []
    lines.extend([f"- {item}" for item in hard_stops] or ["- none"])
    lines += ["", "## Reasons"]
    lines.extend([f"- {item}" for item in (payload.get("reasons") or [])] or ["- none"])
    lines += ["", "## Field actions"]
    actions = payload.get("field_actions") or {}
    lines.extend([f"- {field}: {action}" for field, action in actions.items()] or ["- none"])
    lines += ["", f"Next step: {payload.get('next_step')}", ""]
    return "\n".join(lines)


def coverage_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# PIT Value-Trap Field Coverage",
        "",
        f"Rows: {payload.get('row_count')}",
        f"Final expression coverage: {payload.get('final_expression_coverage')}",
        "",
        "| Field | Exists | Coverage | Non-null rows |",
        "|---|---:|---:|---:|",
    ]
    for row in payload.get("fields", []):
        lines.append(f"| {row.get('field')} | {row.get('exists')} | {row.get('coverage')} | {row.get('non_null_rows')} |")
    return "\n".join(lines) + "\n"


def write_attribution_reports(
    *,
    run_dir: str | Path = DEFAULT_RUN_DIR,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    fields: list[str] | None = None,
    baseline_field: str = DEFAULT_BASELINE_FIELD,
) -> dict[str, Any]:
    run_dir = Path(run_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(run_dir / "dataset.csv")
    fields = fields or DEFAULT_FIELDS
    coverage = build_field_coverage_report(df, fields, final_fields=[baseline_field, *fields])
    distribution = build_distribution_report(df, fields)
    ic = build_single_field_ic_report(df, fields, baseline_field=baseline_field)
    bucket = build_single_field_bucket_report(df, fields, baseline_field=baseline_field)
    scaling = build_scaling_report(df, fields)
    incremental = build_incremental_contribution_report(df, fields, baseline_field=baseline_field)
    correlation = build_field_correlation_report(df, fields, baseline_field=baseline_field)
    missing = build_missing_value_treatment_report(df, fields, baseline_field=baseline_field)
    decision = build_final_decision(coverage=coverage, ic=ic, scaling=scaling, incremental=incremental)

    outputs = {
        "field_coverage_report.json": coverage,
        "field_distribution_report.json": distribution,
        "single_field_ic_report.json": ic,
        "single_field_bucket_report.json": bucket,
        "scaling_winsorize_report.json": scaling,
        "incremental_contribution_report.json": incremental,
        "field_correlation_report.json": correlation,
        "missing_value_treatment_report.json": missing,
        "final_attribution_decision.json": decision,
    }
    for name, payload in outputs.items():
        _write_json(output_dir / name, payload)
    (output_dir / "field_coverage_report.md").write_text(coverage_to_markdown(coverage), encoding="utf-8")
    (output_dir / "final_attribution_decision.md").write_text(decision_to_markdown(decision), encoding="utf-8")
    return {"output_dir": str(output_dir), "decision": decision, "coverage": coverage, "ic": ic, "incremental": incremental}
