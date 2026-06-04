from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
METRIC_ALIASES = {
    "rank_ic_mean": ["rank_ic_mean", "mean_rank_ic", "ic_mean"],
    "rank_ic_ir": ["rank_ic_ir", "ic_ir"],
    "bucket_pair_spread_net": ["bucket_pair_spread_net", "net_spread", "spread_net"],
    "sharpe_net": ["sharpe_net", "sharpe", "annualized_sharpe"],
    "max_drawdown": ["max_drawdown", "mdd"],
    "turnover": ["turnover", "avg_turnover"],
    "coverage": ["coverage", "field_coverage"],
}
FAILURE_CLASSES = {
    "coverage_too_low", "missing_required_fields", "unsupported_feature_requested", "duplicate_equivalent_experiment",
    "future_data_or_timing_risk", "neutralization_breaks_signal", "too_many_split_failures", "bucket_shape_middle_hump",
    "direction_error", "portfolio_construction_mismatch", "cost_sensitivity_failure", "drawdown_too_deep",
    "negative_return_after_cost", "horizon_mismatch", "universe_mismatch", "mechanism_failed", "execution_failure", "manual_review_required",
}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _deep_values(data: Any, key: str) -> Any:
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for value in data.values():
            found = _deep_values(value, key)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _deep_values(item, key)
            if found is not None:
                return found
    return None


def _extract_metrics(blobs: list[dict[str, Any]]) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for canonical, aliases in METRIC_ALIASES.items():
        for blob in blobs:
            for alias in aliases:
                value = _deep_values(blob, alias)
                if value is not None:
                    try:
                        metrics[canonical] = float(value)
                    except Exception:
                        pass
                    break
            if canonical in metrics:
                break
    return metrics


def _status_from(run_dir: Path, blobs: list[dict[str, Any]]) -> str:
    for blob in blobs:
        status = blob.get("status")
        if status:
            if status == "ok":
                return "finished"
            return str(status)
    return "missing_result" if not any((run_dir / name).exists() for name in ["result.json", "results.json"]) else "finished"


def _failure_class(status: str, metrics: dict[str, float], blobs: list[dict[str, Any]]) -> str | None:
    text = json.dumps(blobs).lower()
    if "manual_review" in text:
        return "manual_review_required"
    if "missing_fields" in text or "blocked_missing_data" in text or "blocked_missing_fields" in text:
        return "missing_required_fields"
    if status in {"unsupported_experiment_type", "unsupported"}:
        return "unsupported_feature_requested"
    if "duplicate" in text:
        return "duplicate_equivalent_experiment"
    coverage = metrics.get("coverage")
    if coverage is not None and coverage < 0.8:
        return "coverage_too_low"
    if metrics.get("max_drawdown") is not None and metrics["max_drawdown"] < -0.35:
        return "drawdown_too_deep"
    if metrics.get("bucket_pair_spread_net") is not None and metrics["bucket_pair_spread_net"] < 0:
        return "negative_return_after_cost"
    if status not in {"finished", "ok", "dry_run"}:
        return "execution_failure"
    if not metrics and status != "dry_run":
        return "execution_failure"
    return None


def _quality(metrics: dict[str, float], failure: str | None, blobs: list[dict[str, Any]]) -> dict[str, str]:
    text = json.dumps(blobs).lower()
    return {
        "oos_status": "pass" if metrics.get("rank_ic_mean", 0.0) >= 0.02 or metrics.get("sharpe_net", 0.0) >= 0.5 else "unknown" if not metrics else "fail",
        "cost_status": "pass" if metrics.get("bucket_pair_spread_net", 0.0) > 0 or metrics.get("sharpe_net", 0.0) >= 0.5 else "unknown" if not metrics else "fail",
        "duplicate_status": "duplicate" if failure == "duplicate_equivalent_experiment" else "independent_followup",
        "data_quality_status": "fail" if failure in {"missing_required_fields", "coverage_too_low"} else "pass",
    }


def _information_gain(status: str, failure: str | None, metrics: dict[str, float]) -> str:
    if failure == "missing_required_fields":
        return "blocked_missing_data"
    if failure == "duplicate_equivalent_experiment":
        return "duplicate_or_low_information"
    if failure in {"execution_failure", "unsupported_feature_requested"} or status == "dry_run":
        return "execution_failure" if status != "dry_run" else "duplicate_or_low_information"
    if failure in {"coverage_too_low", "drawdown_too_deep", "negative_return_after_cost"}:
        return "negative_but_informative"
    if metrics.get("rank_ic_mean", 0.0) >= 0.02 or metrics.get("sharpe_net", 0.0) >= 0.5 or metrics.get("bucket_pair_spread_net", 0.0) > 0:
        return "positive_progress"
    return "negative_but_informative" if metrics else "execution_failure"


def build_evidence_ledger(manifest: dict[str, Any]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    for exp in manifest.get("experiments", []):
        run_dir = Path(exp.get("output_dir", ""))
        blobs = [_load_json(run_dir / name) for name in ["status.json", "result.json", "results.json", "factor_evaluations.json", "portfolio_results.json"]]
        metrics = _extract_metrics(blobs)
        status = _status_from(run_dir, blobs)
        failure = _failure_class(status, metrics, blobs)
        if failure not in FAILURE_CLASSES:
            failure = None
        quality = _quality(metrics, failure, blobs)
        row = {
            "experiment_id": exp.get("experiment_id"),
            "status": status,
            "mechanism_id": exp.get("mechanism_id") or (exp.get("spec") or {}).get("mechanism_id"),
            "metrics": metrics,
            "evidence_quality": quality,
            "failure_class": failure,
            "information_gain": _information_gain(status, failure, metrics),
            "manual_review_required": failure == "manual_review_required",
        }
        evidence.append(row)
    return {
        "schema_version": 1,
        "cycle_id": manifest.get("cycle_id"),
        "evidence": evidence,
        "summary": {
            "evidence_count": len(evidence),
            "positive_progress_count": sum(1 for row in evidence if row.get("information_gain") == "positive_progress"),
            "manual_review_required": any(row.get("manual_review_required") for row in evidence),
        },
    }


def write_evidence_ledger(*, root: str | Path = ROOT, cycle_id: str = "cycle_0001") -> dict[str, Any]:
    cycle_dir = Path(root) / "artifacts/harvest_agent" / cycle_id
    manifest_path = cycle_dir / "execution_manifest.json"
    manifest = _load_json(manifest_path) or {"cycle_id": cycle_id, "experiments": []}
    ledger = build_evidence_ledger(manifest)
    (cycle_dir / "evidence_ledger.json").write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (cycle_dir / "evidence_ledger.md").write_text("# Harvest Evidence Ledger\n\n```json\n" + json.dumps(ledger, indent=2) + "\n```\n", encoding="utf-8")
    return ledger
