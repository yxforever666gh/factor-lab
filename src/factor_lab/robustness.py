from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.factor_candidates import summarize_candidate_status
from factor_lab.storage import ExperimentStore


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def aggregate_candidate_checks(
    candidate: dict[str, Any],
    evaluations: list[dict[str, Any]],
    family_context: dict[str, Any] | None = None,
    candidate_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    family_context = family_context or {}
    candidate_context = candidate_context or {}
    latest_eval = sorted(evaluations, key=lambda row: row.get("created_at_utc") or "")[-1] if evaluations else {}
    avg_score = float(candidate.get("avg_final_score") or 0.0)
    latest_score = float(candidate.get("latest_recent_final_score") or candidate.get("latest_final_score") or 0.0)
    pass_rate = float(candidate.get("pass_rate") or 0.0)
    evaluation_count = int(candidate.get("evaluation_count") or len(evaluations) or 0)
    split_fail_count = max(int(row.get("split_fail_count") or 0) for row in evaluations) if evaluations else 0
    high_corr_peer_count = max(int(row.get("high_corr_peer_count") or 0) for row in evaluations) if evaluations else 0
    robust_total = max(int(row.get("robust_total_count") or 0) for row in evaluations) if evaluations else 0
    robust_pass = max(int(row.get("robust_pass_count") or 0) for row in evaluations) if evaluations else 0
    cluster = candidate_context.get("cluster") or {}
    cluster_size = int(cluster.get("cluster_size") or 1)
    related = candidate_context.get("related_candidates") or []
    lineage = candidate_context.get("lineage") or []
    family_score = float(family_context.get("family_score") or 0.0)
    family_action = family_context.get("recommended_action") or "explore_new_branch"
    family_risk_profile = family_context.get("family_risk_profile") or {}
    trial_pressure = float(family_risk_profile.get("trial_pressure") or family_context.get("trial_pressure") or 0.0)
    false_positive_pressure = float(family_risk_profile.get("false_positive_pressure") or family_context.get("false_positive_pressure") or 0.0)

    checks = []

    sample_score = _clip(evaluation_count / 6.0, 0.0, 1.0)
    checks.append({
        "check_name": "evaluation_depth",
        "status": "pass" if evaluation_count >= 3 else "warn" if evaluation_count >= 1 else "fail",
        "severity": "medium" if evaluation_count >= 1 else "high",
        "score": round(sample_score, 6),
        "weight": 1.25,
        "evidence": {"evaluation_count": evaluation_count},
        "rationale": "More windows and reruns reduce single-run illusion risk.",
    })

    consistency_score = _clip((pass_rate + max(avg_score, 0.0) / 3.0) / 2.0, 0.0, 1.0)
    checks.append({
        "check_name": "score_consistency",
        "status": "pass" if pass_rate >= 0.6 and avg_score >= 1.0 else "warn" if pass_rate >= 0.3 or latest_score >= 1.0 else "fail",
        "severity": "medium",
        "score": round(consistency_score, 6),
        "weight": 1.5,
        "evidence": {"pass_rate": pass_rate, "avg_final_score": avg_score, "latest_final_score": latest_score},
        "rationale": "Candidates that repeatedly pass with decent scores are less brittle.",
    })

    robustness_ratio = robust_pass / max(robust_total, 1)
    checks.append({
        "check_name": "window_robustness",
        "status": "pass" if robust_total and robustness_ratio >= 0.6 and split_fail_count == 0 else "warn" if robustness_ratio >= 0.34 else "fail",
        "severity": "high" if split_fail_count >= 2 else "medium",
        "score": round(_clip(robustness_ratio - split_fail_count * 0.18, 0.0, 1.0), 6),
        "weight": 1.7,
        "evidence": {"robust_pass_count": robust_pass, "robust_total_count": robust_total, "split_fail_count": split_fail_count},
        "rationale": "Repeated split failures are an early sign of unstable alpha.",
    })

    corr_score = _clip(1.0 - high_corr_peer_count / 4.0, 0.0, 1.0)
    duplicate_like = len([row for row in lineage if row.get("relationship_type") in {"duplicate_of", "refinement_of"}])
    checks.append({
        "check_name": "graph_crowding",
        "status": "pass" if high_corr_peer_count == 0 and cluster_size <= 2 and duplicate_like == 0 else "warn" if high_corr_peer_count <= 2 and cluster_size <= 4 else "fail",
        "severity": "medium",
        "score": round(_clip((corr_score + _clip(1.0 - duplicate_like / 3.0, 0.0, 1.0)) / 2.0, 0.0, 1.0), 6),
        "weight": 1.15,
        "evidence": {"high_corr_peer_count": high_corr_peer_count, "cluster_size": cluster_size, "lineage_count": len(lineage), "relationship_count": len(related)},
        "rationale": "Crowded graph neighborhoods increase redundancy and false confidence risk.",
    })

    family_ctx_score = _clip((family_score / 100.0), 0.0, 1.0)
    checks.append({
        "check_name": "family_context",
        "status": "pass" if family_score >= 70 and family_action == "continue" else "warn" if family_score >= 45 else "fail",
        "severity": "low" if family_score >= 45 else "medium",
        "score": round(family_ctx_score, 6),
        "weight": 0.9,
        "evidence": {"family_score": family_score, "recommended_action": family_action, "family": candidate.get("family")},
        "rationale": "Healthy families with viable representatives make a candidate easier to trust and extend.",
    })

    trial_score = _clip(1.0 - (trial_pressure / 100.0) * 0.7 - (false_positive_pressure / 100.0) * 0.3, 0.0, 1.0)
    checks.append({
        "check_name": "trial_accounting",
        "status": "pass" if trial_pressure < 35 and false_positive_pressure < 30 else "warn" if trial_pressure < 65 and false_positive_pressure < 60 else "fail",
        "severity": "medium" if trial_pressure < 65 else "high",
        "score": round(trial_score, 6),
        "weight": 1.1,
        "evidence": {"trial_pressure": trial_pressure, "false_positive_pressure": false_positive_pressure, "family": candidate.get("family")},
        "rationale": "Families with many low-information retries should be treated as higher false-positive risk.",
    })

    return checks


def build_candidate_risk_profile(
    candidate: dict[str, Any],
    evaluations: list[dict[str, Any]],
    family_context: dict[str, Any] | None = None,
    candidate_context: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    checks = aggregate_candidate_checks(candidate, evaluations, family_context, candidate_context)
    total_weight = sum(float(row.get("weight") or 0.0) for row in checks) or 1.0
    weighted = sum(float(row.get("score") or 0.0) * float(row.get("weight") or 0.0) for row in checks)
    robustness_score = round(weighted / total_weight, 6)
    family_context_score = round(float((family_context or {}).get("family_score") or 0.0) / 100.0, 6)
    cluster = (candidate_context or {}).get("cluster") or {}
    graph_penalty = min((int(cluster.get("cluster_size") or 1) - 1) * 0.08, 0.35)
    graph_context_score = round(_clip(robustness_score - graph_penalty, 0.0, 1.0), 6)
    fail_count = len([row for row in checks if row.get("status") == "fail"])
    warn_count = len([row for row in checks if row.get("status") == "warn"])
    status_summary = summarize_candidate_status(evaluations)
    fragility = status_summary.get("fragility") or {}
    fragility_penalty = float(fragility.get("risk_score") or 0.0) * 0.35
    risk_score = round(_clip((1.0 - robustness_score) * 100 + fail_count * 9 + warn_count * 3 + graph_penalty * 20 + fragility_penalty, 0.0, 100.0), 6)
    if risk_score >= 70:
        risk_level = "high"
    elif risk_score >= 40:
        risk_level = "medium"
    else:
        risk_level = "low"

    key_risks = [
        f"{row['check_name']}: {row.get('rationale')}"
        for row in checks if row.get("status") in {"fail", "warn"}
    ][:5]
    mitigations = []
    if fail_count or warn_count:
        mitigations.append("Run more independent windows before promoting the candidate.")
    if any(row["check_name"] == "graph_crowding" and row.get("status") != "pass" for row in checks):
        mitigations.append("Prefer cluster representative variants and suppress near-duplicates.")
    if any(row["check_name"] == "window_robustness" and row.get("status") != "pass" for row in checks):
        mitigations.append("Inspect split failures and neutralization sensitivity before expansion.")
    if any(row["check_name"] == "family_context" and row.get("status") == "fail" for row in checks):
        mitigations.append("De-prioritize the family until a stronger branch leader emerges.")

    return {
        "candidate_id": candidate["id"],
        "run_id": run_id,
        "risk_level": risk_level,
        "risk_score": risk_score,
        "robustness_score": robustness_score,
        "family_context_score": family_context_score,
        "graph_context_score": graph_context_score,
        "evaluation_count": int(candidate.get("evaluation_count") or len(evaluations)),
        "passing_check_count": len([row for row in checks if row.get("status") == "pass"]),
        "failing_check_count": fail_count,
        "summary": f"{candidate.get('name')} risk={risk_level} ({risk_score:.1f}) with robustness={robustness_score:.2f} across {len(checks)} checks.",
        "key_risks": key_risks,
        "mitigations": mitigations,
        "checks": checks,
        "fragile": bool(fragility.get("is_fragile")),
        "fragility": fragility,
        "acceptance_gate": status_summary.get("acceptance_gate") or {},
        "acceptance_gate_explanation": status_summary.get("acceptance_gate_explanation"),
        "candidate_status": status_summary.get("status") or candidate.get("status"),
    }


def refresh_candidate_risk_profiles(store: ExperimentStore, run_id: str | None = None, output_dir: str | Path | None = None) -> dict[str, Any]:
    candidates = store.list_factor_candidates(limit=2000)
    evaluations = store.list_factor_evaluations(limit=10000)
    relationships = store.list_candidate_relationships(limit=10000)
    from factor_lab.candidate_graph import build_candidate_graph_context

    graph_context = build_candidate_graph_context(candidates, evaluations, relationships)
    family_by_name = {row.get("family"): row for row in graph_context.get("families", [])}
    candidate_ctx_by_id = {row.get("candidate_id"): row for row in graph_context.get("candidate_context", [])}
    evals_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for row in evaluations:
        evals_by_candidate.setdefault(row.get("candidate_id"), []).append(row)

    profiles = []
    for candidate in candidates:
        family_context = family_by_name.get(candidate.get("family") or "other") or {}
        candidate_context = candidate_ctx_by_id.get(candidate["id"]) or {}
        profile = build_candidate_risk_profile(candidate, evals_by_candidate.get(candidate["id"], []), family_context, candidate_context, run_id=run_id)
        store.replace_candidate_risk_profile(candidate["id"], profile)
        profiles.append({
            "candidate_id": candidate["id"],
            "candidate_name": candidate.get("name"),
            "family": candidate.get("family"),
            **profile,
        })

    profiles.sort(key=lambda row: (-float(row.get("risk_score") or 0.0), row.get("candidate_name") or ""))
    family_summary: dict[str, dict[str, Any]] = {}
    for row in profiles:
        family = row.get("family") or "other"
        bucket = family_summary.setdefault(family, {"family": family, "candidate_count": 0, "risk_score_total": 0.0, "high_risk_count": 0, "medium_risk_count": 0, "low_risk_count": 0, "top_risk_candidates": []})
        bucket["candidate_count"] += 1
        bucket["risk_score_total"] += float(row.get("risk_score") or 0.0)
        bucket[f"{row.get('risk_level')}_risk_count"] += 1
        bucket["top_risk_candidates"].append({"candidate_name": row.get("candidate_name"), "risk_level": row.get("risk_level"), "risk_score": row.get("risk_score")})
    family_rows = []
    for family, bucket in family_summary.items():
        bucket["avg_risk_score"] = round(bucket["risk_score_total"] / max(bucket["candidate_count"], 1), 6)
        bucket["top_risk_candidates"] = sorted(bucket["top_risk_candidates"], key=lambda row: -float(row.get("risk_score") or 0.0))[:5]
        del bucket["risk_score_total"]
        family_rows.append(bucket)
    family_rows.sort(key=lambda row: (-float(row.get("avg_risk_score") or 0.0), row.get("family") or ""))

    payload = {"profiles": profiles, "family_risk_summary": family_rows, "generated_from_run_id": run_id}
    if output_dir is not None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "candidate_risk_profiles.json").write_text(json.dumps(profiles, ensure_ascii=False, indent=2), encoding="utf-8")
        (output_dir / "candidate_risk_family_summary.json").write_text(json.dumps(family_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        (output_dir / "candidate_risk_snapshot.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
