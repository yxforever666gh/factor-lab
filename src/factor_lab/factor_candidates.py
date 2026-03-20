from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any


GOOD_STATUSES = {"promising", "testing"}
BAD_STATUSES = {"rejected", "archived"}
OFFICIAL_RUN_SCOPES = {"official", "generated", "batch_official"}


def infer_factor_family(name: str, expression: str | None = None) -> str:
    haystack = f"{name} {expression or ''}".lower()
    if "mom" in haystack or "momentum" in haystack:
        return "momentum"
    if any(token in haystack for token in ["value", "yield", "ep", "bp"]):
        return "value"
    if any(token in haystack for token in ["vol", "variance", "std", "atr"]):
        return "volatility"
    if any(token in haystack for token in ["liq", "turnover", "volume"]):
        return "liquidity"
    if any(token in haystack for token in ["quality", "profit", "roe", "margin"]):
        return "quality"
    if "+" in haystack or "combo" in haystack or "hybrid" in haystack:
        return "hybrid"
    return "other"


def derive_window_label(config_path: str | None, start_date: str | None, end_date: str | None) -> str:
    config_path = config_path or ""
    if "recent" in config_path:
        return "recent"
    if "expanding" in config_path:
        return "expanding"
    if "rolling_" in config_path:
        stem = config_path.rsplit("/", 1)[-1].replace(".json", "")
        return stem
    if start_date and end_date:
        return f"{start_date}->{end_date}"
    return config_path or "unknown"


def score_candidate_evaluation(metrics: dict[str, Any]) -> dict[str, Any]:
    sample_size = int(metrics.get("sample_size") or 0)
    return_metric = float(metrics.get("return_metric") or 0.0)
    sharpe_like = float(metrics.get("sharpe_like") or 0.0)
    max_drawdown = float(metrics.get("max_drawdown") or 0.0)
    turnover = float(metrics.get("turnover") or 0.0)
    coverage = float(metrics.get("coverage") or 0.0)
    raw_ic = float(metrics.get("raw_rank_ic_mean") or 0.0)
    neutral_ic = float(metrics.get("neutralized_rank_ic_mean") or 0.0)
    split_fail_count = int(metrics.get("split_fail_count") or 0)
    high_corr_peer_count = int(metrics.get("high_corr_peer_count") or 0)
    observations = int(metrics.get("observations") or 0)
    robust_pass_count = int(metrics.get("robust_pass_count") or 0)
    robust_total_count = int(metrics.get("robust_total_count") or 0)
    run_scope = metrics.get("run_scope") or "official"

    clipped_return = max(min(return_metric, 1.5), -1.5)
    clipped_sharpe = max(min(sharpe_like, 4.0), -2.0)
    drawdown_penalty = abs(min(max_drawdown, 0.0))
    scope_multiplier = 0.35 if run_scope == "demo" else 1.0

    stability_score = (
        raw_ic * 3.2
        + max(neutral_ic, -0.1) * 2.4
        + clipped_sharpe * 0.35
        + min(coverage, 1.0) * 0.9
        + min(sample_size / 240.0, 1.2) * 0.7
        + min(observations / 240.0, 1.2) * 0.55
        + (robust_pass_count / max(robust_total_count, 1)) * 0.9
        - split_fail_count * 0.55
        - high_corr_peer_count * 0.12
    ) * scope_multiplier

    quality_score = (
        clipped_return * 1.4
        + clipped_sharpe * 0.55
        + max(raw_ic, -0.1) * 2.1
        + max(neutral_ic, -0.1) * 1.8
        - drawdown_penalty * 4.0
        - turnover * 0.45
    ) * scope_multiplier

    final_score = stability_score + quality_score
    rejection_reasons: list[str] = []
    if sample_size < 80:
        rejection_reasons.append("sample_too_small")
    if observations < 40:
        rejection_reasons.append("insufficient_observations")
    if coverage < 0.2:
        rejection_reasons.append("coverage_too_low")
    if max_drawdown < -0.30:
        rejection_reasons.append("drawdown_too_deep")
    if split_fail_count >= 2:
        rejection_reasons.append("too_many_split_failures")
    if neutral_ic < 0 and raw_ic > 0:
        rejection_reasons.append("neutralization_breaks_signal")
    if sharpe_like < 0:
        rejection_reasons.append("negative_sharpe")
    if return_metric < 0:
        rejection_reasons.append("negative_return")

    pass_flag = not rejection_reasons and final_score >= 1.0
    if pass_flag and final_score >= 2.4:
        status = "promising"
    elif pass_flag:
        status = "testing"
    elif final_score <= -0.75 or len(rejection_reasons) >= 2:
        status = "rejected"
    else:
        status = "archived"

    return {
        "stability_score": round(stability_score, 6),
        "quality_score": round(quality_score, 6),
        "final_score": round(final_score, 6),
        "pass_flag": int(pass_flag),
        "status": status,
        "rejection_reason": "; ".join(rejection_reasons) if rejection_reasons else None,
    }


def summarize_candidate_status(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    if not evaluations:
        return {
            "status": "new",
            "evaluation_count": 0,
            "window_count": 0,
            "avg_final_score": None,
            "best_final_score": None,
            "latest_final_score": None,
            "pass_rate": None,
            "summary": "No evaluations yet.",
            "next_action": "seed_validation",
            "rejection_reason": None,
        }

    evaluations = sorted(evaluations, key=lambda row: row.get("created_at_utc") or "")
    scores = [float(row.get("final_score") or 0.0) for row in evaluations]
    windows = {row.get("window_label") or "unknown" for row in evaluations}
    latest_score = round(scores[-1], 6)
    best_score = round(max(scores), 6)

    official_evaluations = [
        row for row in evaluations
        if (row.get("notes") or {}).get("run_scope") in OFFICIAL_RUN_SCOPES
    ]
    status_pool = official_evaluations or evaluations
    pass_flags = [int(row.get("pass_flag") or 0) for row in status_pool]
    statuses = [row.get("status") or "testing" for row in status_pool]
    official_scores = [float(row.get("final_score") or 0.0) for row in status_pool]
    official_windows = {row.get("window_label") or "unknown" for row in status_pool}
    status_counter = Counter(statuses)
    avg_score = round(sum(official_scores) / len(status_pool), 6)
    pass_rate = round(sum(pass_flags) / len(status_pool), 4)
    official_eval_count = len(status_pool)
    official_window_count = len(official_windows)

    if official_eval_count >= 5 and official_window_count >= 3 and pass_rate >= 0.6 and avg_score >= 1.0:
        status = "promising"
        next_action = "refine"
    elif official_eval_count >= 2 and official_window_count >= 1 and (pass_rate >= 0.2 or avg_score >= 0.5 or best_score >= 1.5):
        status = "testing"
        next_action = "validate_more_windows"
    elif official_eval_count <= 1 and len(evaluations) >= 1:
        status = "testing"
        next_action = "validate_more_windows"
    elif status_counter.get("archived", 0) >= 2 and status_counter.get("promising", 0) == 0:
        status = "archived"
        next_action = "low_priority_retest"
    else:
        status = "rejected"
        next_action = "stop"

    rejection_reasons = [row.get("rejection_reason") for row in status_pool if row.get("rejection_reason")]
    rejection_reason = rejection_reasons[-1] if rejection_reasons else None
    summary = (
        f"{len(evaluations)} evals ({official_eval_count} official) across {len(windows)} windows "
        f"({official_window_count} official); avg_score={avg_score}, latest={scores[-1]:.3f}, pass_rate={pass_rate:.2f}."
    )
    return {
        "status": status,
        "evaluation_count": len(evaluations),
        "window_count": len(windows),
        "avg_final_score": avg_score,
        "best_final_score": best_score,
        "latest_final_score": latest_score,
        "pass_rate": pass_rate,
        "summary": summary,
        "next_action": next_action,
        "rejection_reason": rejection_reason,
    }


def build_hypothesis_summary(candidate: dict[str, Any], evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    family = candidate.get("family") or infer_factor_family(candidate.get("name", ""), None)
    promising_windows = [row.get("window_label") for row in evaluations if row.get("status") == "promising"]
    rejected_windows = [row.get("window_label") for row in evaluations if row.get("status") in BAD_STATUSES]
    evidence_for = []
    evidence_against = []
    if promising_windows:
        evidence_for.append(f"promising windows: {', '.join(promising_windows[:4])}")
    high_scores = [row for row in evaluations if float(row.get("final_score") or 0.0) >= 2.0]
    if high_scores:
        evidence_for.append(f"high score count: {len(high_scores)}")
    if rejected_windows:
        evidence_against.append(f"weak windows: {', '.join(rejected_windows[:4])}")
    recent_reason = next((row.get("rejection_reason") for row in reversed(evaluations) if row.get("rejection_reason")), None)
    if recent_reason:
        evidence_against.append(recent_reason)

    candidate_status = candidate.get("status") or "testing"
    if candidate_status == "promising":
        next_action = "expand same family with nearby variants"
    elif candidate_status == "rejected":
        next_action = "stop expanding this branch"
    elif candidate_status == "archived":
        next_action = "only retest if stronger context appears"
    else:
        next_action = "collect more validation windows"

    title = f"{family} hypothesis: {candidate.get('name')}"
    hypothesis_text = (
        f"Factor {candidate.get('name')} from family {family} is being tested for stable alpha across rolling and expanding windows."
    )
    return {
        "title": title,
        "family": family,
        "hypothesis_text": hypothesis_text,
        "status": candidate_status,
        "evidence_for_json": json.dumps(evidence_for, ensure_ascii=False),
        "evidence_against_json": json.dumps(evidence_against, ensure_ascii=False),
        "next_action": next_action,
    }


def grouped_evaluation_windows(evaluations: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evaluations:
        grouped[row.get("window_label") or "unknown"].append(row)
    return dict(grouped)
