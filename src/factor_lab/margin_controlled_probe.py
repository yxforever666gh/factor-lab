from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import (
    DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD,
    MarginFeatureBuildConfig,
    bucket_spread,
    spearman_corr,
)

SCORE_COLUMNS = ("value_quality_baseline", "low_margin_crowding", "margin_low_crowding_confirmation")


def load_margin_feature_sample(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"date", "ticker", "forward_return_5d", *SCORE_COLUMNS}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"].astype(str), format="%Y%m%d", errors="coerce").dt.strftime("%Y%m%d")
    for col in ["forward_return_5d", *SCORE_COLUMNS]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.dropna(subset=["date", "ticker", "forward_return_5d", *SCORE_COLUMNS])


def _evaluate_scores(frame: pd.DataFrame, cfg: MarginFeatureBuildConfig) -> dict[str, Any]:
    scores: dict[str, Any] = {}
    for score in SCORE_COLUMNS:
        spread = bucket_spread(
            frame,
            score,
            quantiles=cfg.quantiles,
            long_quantile=cfg.long_quantile,
            short_quantile=cfg.short_quantile,
        )
        spread["rank_ic"] = spearman_corr(frame[score], frame["forward_return_5d"])
        scores[score] = spread
    return scores


def _split_frame(frame: pd.DataFrame, *, holdout_start: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(frame["date"], format="%Y%m%d", errors="coerce")
    holdout_ts = pd.Timestamp(holdout_start)
    return frame.loc[dates < holdout_ts].copy(), frame.loc[dates >= holdout_ts].copy()


def build_margin_controlled_probe(
    sample_path: str | Path = "artifacts/margin_feature_sample/margin_feature_sample.csv",
    *,
    holdout_start: str = "2023-01-01",
    config: MarginFeatureBuildConfig | None = None,
) -> dict[str, Any]:
    cfg = config or MarginFeatureBuildConfig(benchmark_spread=DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD)
    frame = load_margin_feature_sample(sample_path)
    train, holdout = _split_frame(frame, holdout_start=holdout_start)
    full_scores = _evaluate_scores(frame, cfg)
    train_scores = _evaluate_scores(train, cfg) if not train.empty else {}
    holdout_scores = _evaluate_scores(holdout, cfg) if not holdout.empty else {}

    confirmation = holdout_scores.get("margin_low_crowding_confirmation", {})
    baseline = holdout_scores.get("value_quality_baseline", {})
    full_confirmation = full_scores.get("margin_low_crowding_confirmation", {})
    reasons: list[str] = []

    conf_spread = confirmation.get("spread_mean")
    base_spread = baseline.get("spread_mean")
    conf_positive = confirmation.get("spread_positive_rate")
    full_conf_spread = full_confirmation.get("spread_mean")

    if not holdout_scores or (confirmation.get("observations") or 0) < cfg.min_overlap_dates:
        reasons.append("holdout_observations_too_few")
    if conf_spread is None or conf_spread <= 0:
        reasons.append("holdout_confirmation_non_positive")
    if conf_spread is not None and base_spread is not None and conf_spread <= base_spread:
        reasons.append("holdout_confirmation_not_incremental_vs_baseline")
    if conf_spread is None or conf_spread <= cfg.benchmark_spread:
        reasons.append("holdout_confirmation_not_above_benchmark")
    if conf_positive is None or conf_positive < 0.55:
        reasons.append("holdout_positive_rate_below_threshold")

    if not reasons:
        decision = "strong_pass_prepare_workflow_integration"
    elif (
        full_conf_spread is not None
        and full_conf_spread > cfg.benchmark_spread
        and conf_spread is not None
        and conf_spread > 0
        and "holdout_confirmation_not_incremental_vs_baseline" not in reasons
    ):
        decision = "weak_pass_extend_margin_sampling"
    else:
        decision = "fail_stop_margin_low_crowding_probe"

    return {
        "schema_version": 1,
        "sample_path": str(sample_path),
        "holdout_start": holdout_start,
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "coverage": {
            "rows": int(len(frame)),
            "dates": int(frame["date"].nunique()),
            "tickers": int(frame["ticker"].nunique()),
            "train_rows": int(len(train)),
            "train_dates": int(train["date"].nunique()) if not train.empty else 0,
            "holdout_rows": int(len(holdout)),
            "holdout_dates": int(holdout["date"].nunique()) if not holdout.empty else 0,
        },
        "full_sample": full_scores,
        "train": train_scores,
        "holdout": holdout_scores,
        "decision": {"decision": decision, "reasons": reasons or ["held_out_confirmation_passed_all_thresholds"]},
    }


def to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Margin Controlled Probe",
        "",
        "Scope: held-out diagnostic from bounded margin feature sample. No queue write, no daemon start.",
        "",
        "## Decision",
        f"- Decision: `{payload.get('decision', {}).get('decision')}`",
        f"- Reasons: {', '.join(payload.get('decision', {}).get('reasons', []))}",
        "",
        "## Coverage",
    ]
    coverage = payload.get("coverage", {})
    for key in ["rows", "dates", "tickers", "train_rows", "train_dates", "holdout_rows", "holdout_dates"]:
        lines.append(f"- {key}: {coverage.get(key)}")
    lines += [
        "",
        "## Score diagnostics",
        "| Split | Score | Spread mean | Positive rate | Observations | Rank IC |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for split in ["full_sample", "train", "holdout"]:
        for score in SCORE_COLUMNS:
            rec = payload.get(split, {}).get(score, {})
            lines.append(f"| {split} | {score} | {rec.get('spread_mean')} | {rec.get('spread_positive_rate')} | {rec.get('observations')} | {rec.get('rank_ic')} |")
    return "\n".join(lines) + "\n"
