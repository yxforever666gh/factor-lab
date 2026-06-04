from __future__ import annotations

from typing import Any

DEFAULT_THRESHOLDS = {
    "min_rank_ic_mean": 0.02,
    "min_rank_ic_ir": 0.15,
    "min_sharpe_net": 0.5,
    "max_drawdown_floor": -0.35,
    "min_coverage": 0.8,
}


def _score_bool(value: bool, good: int = 5, bad: int = 1) -> int:
    return good if value else bad


def _clip_score(value: int | float) -> int:
    return max(0, min(5, int(round(value))))


def score_evidence_row(row: dict[str, Any], *, thresholds: dict[str, float] | None = None) -> dict[str, Any]:
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    m = row.get("metrics") or {}
    q = row.get("evidence_quality") or {}
    failure = row.get("failure_class")
    coverage = float(m.get("coverage", 1.0))
    hard_gates = {
        "data_gate": failure not in {"missing_required_fields", "coverage_too_low"} and coverage >= t["min_coverage"],
        "duplicate_gate": q.get("duplicate_status") != "duplicate" and failure != "duplicate_equivalent_experiment",
        "oos_gate": float(m.get("rank_ic_mean", 0.0)) >= t["min_rank_ic_mean"] or q.get("oos_status") == "pass",
        "cost_gate": float(m.get("sharpe_net", 0.0)) >= t["min_sharpe_net"] or float(m.get("bucket_pair_spread_net", 0.0)) > 0,
        "portfolio_construction_gate": failure not in {"portfolio_construction_mismatch", "bucket_shape_middle_hump"},
        "risk_gate": float(m.get("max_drawdown", 0.0)) >= t["max_drawdown_floor"],
        "mechanism_gate": failure != "mechanism_failed" and bool(row.get("mechanism_id") or True),
    }
    soft_scorecard = {
        "mechanism_credibility": _score_bool(hard_gates["mechanism_gate"], 4, 1),
        "data_quality": _score_bool(hard_gates["data_gate"], 5, 1),
        "expected_information_gain": 5 if row.get("information_gain") == "positive_progress" else 3 if row.get("information_gain") == "negative_but_informative" else 1,
        "oos_stability": _clip_score(5 if hard_gates["oos_gate"] else 2),
        "cost_robustness": _clip_score(5 if hard_gates["cost_gate"] else 2),
        "risk_improvement": _clip_score(5 if hard_gates["risk_gate"] else 1),
        "portfolio_construction_fit": _score_bool(hard_gates["portfolio_construction_gate"], 4, 1),
        "novelty_vs_existing_evidence": _score_bool(hard_gates["duplicate_gate"], 4, 0),
        "implementation_reliability": 5 if failure is None else 2 if row.get("status") in {"finished", "ok"} else 1,
        "knowledge_value_even_if_failed": 4 if row.get("information_gain") in {"positive_progress", "negative_but_informative", "blocked_missing_data"} else 1,
    }
    avg = round(sum(soft_scorecard.values()) / len(soft_scorecard), 2)
    eligible = all(hard_gates.values()) and avg >= 4.0 and not row.get("manual_review_required")
    scored = dict(row)
    scored.update({"hard_gates": hard_gates, "soft_scorecard": soft_scorecard, "soft_score_average": avg, "promotion_eligible": eligible})
    return scored


def score_evidence_ledger(ledger: dict[str, Any], *, thresholds: dict[str, float] | None = None) -> dict[str, Any]:
    rows = [score_evidence_row(row, thresholds=thresholds) for row in ledger.get("evidence", [])]
    out = dict(ledger)
    out["evidence"] = rows
    out["summary"] = {**(ledger.get("summary") or {}), "promotion_eligible_count": sum(1 for row in rows if row.get("promotion_eligible")), "average_soft_score": round(sum(row.get("soft_score_average", 0) for row in rows) / len(rows), 2) if rows else 0.0}
    return out
