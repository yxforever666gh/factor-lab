from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.candidate_failure_dossier import build_candidate_failure_dossiers
from factor_lab.storage import ExperimentStore
from factor_lab.novelty_judge import load_novelty_judgments


_FRONTIER_PASS_STATUSES = {"pass"}
_FRONTIER_VALIDATION_STATUSES = {"monitor", "blocked"}


_DECISION_PRIORITY = {
    "core_candidate": 0,
    "validate_now": 1,
    "dedupe_first": 2,
    "regime_sensitive": 3,
    "watchlist": 4,
    "drop_from_frontier": 5,
}

_DECISION_LABELS = {
    "core_candidate": "保留核心",
    "validate_now": "继续验证",
    "dedupe_first": "先去重",
    "regime_sensitive": "降级为 regime-sensitive",
    "watchlist": "继续观察",
    "drop_from_frontier": "退出前线",
}

_CLASSIFICATION_PRIORITY = {
    "stable-alpha-candidate": 0,
    "needs-validation": 1,
    "exposure-track": 2,
    "regime-sensitive": 3,
    "duplicate-suppress": 4,
    "validate-only": 5,
    "drop": 6,
}

_CLASSIFICATION_LABELS = {
    "stable-alpha-candidate": "稳定 alpha 候选",
    "needs-validation": "继续验证",
    "exposure-track": "Exposure Track",
    "regime-sensitive": "Regime-sensitive",
    "duplicate-suppress": "重复候选压制",
    "validate-only": "仅验证",
    "drop": "淘汰",
}

_PROMOTION_LABELS = {
    "promote": "允许晋升",
    "keep_validating": "继续验证",
    "do_not_promote": "暂不晋升",
    "suppress": "压制重复候选",
    "hold": "暂缓，等待可信证据",
}


def _clip(value: float | None, low: float = 0.0, high: float = 1.0) -> float:
    if value is None:
        return low
    return max(low, min(high, float(value)))


def _latest_finished_run(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT run_id, created_at_utc, config_path, output_dir
        FROM workflow_runs
        WHERE status = 'finished'
          AND COALESCE(config_path, '') NOT LIKE 'artifacts/generated_ab_configs/%'
          AND COALESCE(output_dir, '') NOT LIKE 'artifacts/ab_harness/%'
        ORDER BY created_at_utc DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else None


def _load_latest_factor_map(conn: sqlite3.Connection, run_id: str) -> dict[str, dict[str, dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT factor_name, variant, rank_ic_mean, rank_ic_ir, split_fail_count, high_corr_peers_json, score
        FROM factor_results
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    factor_map: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        payload = dict(row)
        payload["high_corr_peers"] = json.loads(payload.pop("high_corr_peers_json") or "[]")
        factor_map.setdefault(payload["factor_name"], {})[payload["variant"]] = payload
    return factor_map


def _load_latest_exposure_map(conn: sqlite3.Connection, run_id: str) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT factor_name, total_score, status, retention_industry, split_fail_count,
               crowding_peers, recommended_max_weight, bucket_key, bucket_label,
               effective_bucket_key, effective_bucket_label,
               turnover_daily, net_metric, hard_flags_json
        FROM exposure_factors
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    exposure_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(row)
        payload["hard_flags"] = json.loads(payload.pop("hard_flags_json") or "[]")
        exposure_map[payload["factor_name"]] = payload
    return exposure_map


def _load_risk_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT fc.name, rp.risk_level, rp.risk_score, rp.robustness_score,
               rp.passing_check_count, rp.failing_check_count, rp.profile_json
        FROM candidate_risk_profile rp
        LEFT JOIN factor_candidates fc ON fc.id = rp.candidate_id
        """
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not row["name"]:
            continue
        profile = json.loads(row["profile_json"] or "{}")
        out[row["name"]] = {
            "risk_level": row["risk_level"],
            "risk_score": row["risk_score"],
            "robustness_score": row["robustness_score"],
            "passing_check_count": row["passing_check_count"],
            "failing_check_count": row["failing_check_count"],
            "acceptance_gate": profile.get("acceptance_gate") or {},
            "acceptance_gate_explanation": profile.get("acceptance_gate_explanation"),
        }
    return out


def _load_thesis_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT fc.name, rt.thesis_id, rt.title, rt.family, rt.thesis_type,
               rt.institutional_bucket_key, rt.institutional_bucket_label,
               rt.thesis_text, rt.mechanism_rationale, rt.status,
               rt.invalidation_json, rt.representative_candidate, rt.representative_rank,
               rt.representative_count, rt.roster_json, rt.source_context_json
        FROM research_theses rt
        LEFT JOIN factor_candidates fc ON fc.id = rt.candidate_id
        """
    ).fetchall()
    thesis_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not row["name"]:
            continue
        thesis_map[row["name"]] = {
            "thesis_id": row["thesis_id"],
            "title": row["title"],
            "family": row["family"],
            "thesis_type": row["thesis_type"],
            "institutional_bucket_key": row["institutional_bucket_key"],
            "institutional_bucket_label": row["institutional_bucket_label"],
            "thesis_text": row["thesis_text"],
            "mechanism_rationale": row["mechanism_rationale"],
            "status": row["status"],
            "invalidation": json.loads(row["invalidation_json"] or "[]"),
            "representative_candidate": row["representative_candidate"],
            "representative_rank": row["representative_rank"],
            "representative_count": row["representative_count"],
            "roster": json.loads(row["roster_json"] or "[]"),
            "source_context": json.loads(row["source_context_json"] or "{}"),
        }
    return thesis_map


def _load_representative_roster(output_dir: str | None) -> dict[str, dict[str, Any]]:
    if not output_dir:
        return {}
    path = Path(output_dir) / "cluster_representatives.json"
    if not path.exists():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    group_map: dict[tuple[str, ...], dict[str, Any]] = {}
    rep_row_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        members = tuple(sorted(row.get("cluster_members") or [row.get("factor_name")]))
        group = group_map.setdefault(
            members,
            {"cluster_members": list(members), "representative_candidates": [], "primary_candidate": None},
        )
        factor_name = row.get("factor_name")
        if factor_name and factor_name not in group["representative_candidates"]:
            group["representative_candidates"].append(factor_name)
        if row.get("is_primary_representative") and factor_name:
            group["primary_candidate"] = factor_name
        if factor_name:
            rep_row_map[factor_name] = row
    roster_map: dict[str, dict[str, Any]] = {}
    for members, group in group_map.items():
        primary = group.get("primary_candidate") or next(iter(group.get("representative_candidates") or group.get("cluster_members") or []), None)
        reps = list(group.get("representative_candidates") or [])
        suppressed = [name for name in group.get("cluster_members") or [] if name not in reps]
        for member in members:
            rep_row = rep_row_map.get(member) or {}
            roster_map[member] = {
                "primary_candidate": primary,
                "representative_candidates": reps,
                "cluster_members": list(group.get("cluster_members") or []),
                "representative_rank": rep_row.get("cluster_rep_rank") or rep_row.get("representative_rank"),
                "representative_count": rep_row.get("cluster_rep_count") or rep_row.get("representative_count") or len(reps),
                "is_representative": member in reps,
                "is_primary_representative": member == primary,
                "suppressed_members": suppressed,
            }
    return roster_map


def _load_portfolio_contribution_map(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "paper_portfolio" / "portfolio_contribution_report.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload.get("rows") or []
    return {
        row.get("factor_name"): row
        for row in rows
        if row.get("factor_name")
    }


def _load_approved_universe_map(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "approved_candidate_universe.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload.get("rows") or []
    return {
        row.get("factor_name"): {**row, "selection_policy_version": payload.get("selection_policy_version")}
        for row in rows
        if row.get("factor_name")
    }



def _load_relationship_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT l.name AS left_name, r.name AS right_name, cr.relationship_type, cr.strength
        FROM candidate_relationships cr
        LEFT JOIN factor_candidates l ON l.id = cr.left_candidate_id
        LEFT JOIN factor_candidates r ON r.id = cr.right_candidate_id
        """
    ).fetchall()
    relationship_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        left_name = row["left_name"]
        right_name = row["right_name"]
        relationship_type = row["relationship_type"] or "other"
        strength = float(row["strength"] or 0.0)
        for source, peer in ((left_name, right_name), (right_name, left_name)):
            if not source or not peer:
                continue
            bucket = relationship_map.setdefault(source, {
                "duplicate_peers": [],
                "refinement_peers": [],
                "high_corr_peers": [],
            })
            if relationship_type == "duplicate_of":
                bucket["duplicate_peers"].append({"name": peer, "strength": strength})
            elif relationship_type == "refinement_of":
                bucket["refinement_peers"].append({"name": peer, "strength": strength})
            elif relationship_type == "high_corr":
                bucket["high_corr_peers"].append({"name": peer, "strength": strength})
    for bucket in relationship_map.values():
        for key in ("duplicate_peers", "refinement_peers", "high_corr_peers"):
            bucket[key].sort(key=lambda row: (-float(row.get("strength") or 0.0), row.get("name") or ""))
    return relationship_map


def _decide_candidate(row: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    latest_score = float(row.get("latest_recent_final_score") or row.get("latest_final_score") or 0.0)
    window_count = int(row.get("window_count") or 0)
    evaluation_count = int(row.get("evaluation_count") or 0)
    pass_rate = float(row.get("pass_rate") or 0.0)
    risk_score = float(row.get("risk_score") or 100.0)
    robustness_score = float(row.get("robustness_score") or 0.0)
    split_fail_count = int(row.get("split_fail_count") or 0)
    crowding_peers = int(row.get("crowding_peers") or 0)
    duplicate_peer_count = int(row.get("duplicate_peer_count") or 0)
    refinement_peer_count = int(row.get("refinement_peer_count") or 0)
    high_corr_peer_count = int(row.get("high_corr_peer_count") or 0)
    retention_value = row.get("retention_industry")
    retention = float(retention_value or 0.0)

    if window_count < 2:
        reasons.append("跨窗口样本不足")
    elif window_count < 4:
        reasons.append("窗口覆盖还不够厚")
    if evaluation_count < 40:
        reasons.append("独立评估次数偏少")
    if pass_rate < 0.35:
        reasons.append("历史通过率偏低")
    if robustness_score < 0.65:
        reasons.append("稳健性得分不够硬")
    if risk_score >= 70:
        reasons.append("风险画像偏高")
    if split_fail_count >= 1:
        reasons.append("最新轮存在 split fail")
    if crowding_peers >= 2 or high_corr_peer_count >= 2:
        reasons.append("与现有赢家过于拥挤")
    if duplicate_peer_count >= 1:
        reasons.append("存在近重复候选")
    elif refinement_peer_count >= 2:
        reasons.append("存在过多 refinement 变体")
    if retention_value is not None and retention <= 0.15 and latest_score >= 7.0:
        reasons.append("中性化残留太薄")

    if (duplicate_peer_count >= 1 or refinement_peer_count >= 2 or crowding_peers >= 2 or high_corr_peer_count >= 2) and latest_score >= 7.5:
        return "dedupe_first", reasons or ["和现有赢家高度相似，先去重再说"]
    if (
        window_count >= 4
        and pass_rate >= 0.45
        and robustness_score >= 0.7
        and risk_score < 60
        and split_fail_count == 0
        and retention >= 0.2
    ):
        return "core_candidate", reasons or ["跨窗口、中性化和风险约束都过线"]
    if latest_score >= 7.0 and (window_count < 2 or evaluation_count < 40):
        return "validate_now", reasons or ["近期很强，但还没过晋级赛"]
    if latest_score >= 7.0 and (
        pass_rate < 0.35
        or risk_score >= 70
        or split_fail_count >= 1
        or (retention_value is not None and retention <= 0.15)
    ):
        return "regime_sensitive", reasons or ["强度高，但更像 regime 机会而非稳定核心"]
    if latest_score >= 5.0:
        return "watchlist", reasons or ["保留观察价值，但证据还不够"]
    return "drop_from_frontier", reasons or ["暂时退出前线，给更强候选让路"]


def _score_cross_window(candidate: dict[str, Any], risk: dict[str, Any], split_fail_count: int) -> int:
    window_count = int(candidate.get("window_count") or 0)
    pass_rate = float(candidate.get("pass_rate") or 0.0)
    robustness_score = float(risk.get("robustness_score") or 0.0)
    evaluation_count = int(candidate.get("evaluation_count") or 0)

    score = 0.0
    score += _clip(window_count / 6.0) * 14.0
    score += _clip(pass_rate / 0.6) * 8.0
    score += _clip(robustness_score / 0.85) * 8.0
    score -= _clip(split_fail_count / 2.0) * 4.0
    if evaluation_count < 40:
        score -= 2.0
    return int(round(_clip(score, 0.0, 30.0)))


def _score_neutralized_quality(retention: float | None, neutral_ic: float | None, exposure: dict[str, Any]) -> int:
    if retention is None and neutral_ic is None:
        return 6
    retention_support = 0.45 if retention is None else _clip((float(retention) + 0.05) / 0.45)
    neutral_support = _clip(((float(neutral_ic or 0.0) + 0.02) / 0.06))
    hard_flag_penalty = 0.0
    if "b2_retention_industry_too_low" in (exposure.get("hard_flags") or []):
        hard_flag_penalty = 4.0
    score = retention_support * 12.0 + neutral_support * 8.0 - hard_flag_penalty
    return int(round(_clip(score, 0.0, 20.0)))


def _score_incremental_value(
    candidate: dict[str, Any],
    row: dict[str, Any],
    failure_dossier: dict[str, Any],
    portfolio_contribution: dict[str, Any],
    representative_context: dict[str, Any],
) -> int:
    latest_score = float(candidate.get("latest_recent_final_score") or candidate.get("latest_final_score") or 0.0)
    avg_score = float(candidate.get("avg_final_score") or 0.0)
    pass_rate = float(candidate.get("pass_rate") or 0.0)
    exposure_total = float(row.get("exposure_total_score") or 0.0)
    delta_sharpe = float(portfolio_contribution.get("delta_sharpe") or 0.0)
    delta_cost = float(portfolio_contribution.get("delta_cost_adjusted_annual_return") or 0.0)

    score = 0.0
    score += _clip((latest_score - 5.0) / 4.5) * 6.0
    score += _clip((avg_score - 1.5) / 5.0) * 3.0
    score += _clip(pass_rate / 0.5) * 3.0
    score += _clip(exposure_total / 80.0) * 2.0
    if failure_dossier.get("parent_delta_status") == "incremental":
        score += 4.0
    elif failure_dossier.get("parent_delta_status") == "non_incremental":
        score -= 6.0
    score += _clip((delta_sharpe + 0.05) / 0.20) * 4.0
    score += _clip((delta_cost + 0.02) / 0.08) * 2.0
    if representative_context.get("is_primary_representative"):
        score += 2.0
    elif representative_context and not representative_context.get("is_representative", True):
        score -= 4.0
    return int(round(_clip(score, 0.0, 20.0)))


def _score_deduped_independence(duplicate_count: int, refinement_count: int, high_corr_count: int, crowding_peers: int) -> int:
    if duplicate_count >= 1:
        return 0
    score = 15.0
    score -= min(6.0, refinement_count * 2.5)
    score -= min(5.0, high_corr_count * 2.0)
    score -= min(4.0, crowding_peers * 1.5)
    return int(round(_clip(score, 0.0, 15.0)))


def _score_split_consistency(split_fail_count: int, risk: dict[str, Any]) -> int:
    passing_checks = int(risk.get("passing_check_count") or 0)
    failing_checks = int(risk.get("failing_check_count") or 0)
    score = 8.0
    score -= min(6.0, split_fail_count * 3.0)
    score += min(2.0, passing_checks * 0.5)
    score -= min(4.0, failing_checks * 1.25)
    return int(round(_clip(score, 0.0, 10.0)))


def _score_interpretability(name: str, exposure: dict[str, Any], relationships: dict[str, Any]) -> int:
    score = 1.0
    if exposure.get("effective_bucket_label"):
        score += 1.5
    if exposure.get("status"):
        score += 1.0
    if relationships.get("duplicate_peers") or relationships.get("refinement_peers"):
        score += 0.5
    if any(token in (name or "").lower() for token in ("hybrid", "mom", "value", "size", "liquidity", "turnover", "quality")):
        score += 1.0
    return int(round(_clip(score, 0.0, 5.0)))


def _build_quality_scores(
    candidate: dict[str, Any],
    risk: dict[str, Any],
    row: dict[str, Any],
    relationships: dict[str, Any],
    failure_dossier: dict[str, Any],
    portfolio_contribution: dict[str, Any],
    representative_context: dict[str, Any],
) -> dict[str, int]:
    return {
        "cross_window_robustness": _score_cross_window(candidate, risk, int(row.get("split_fail_count") or 0)),
        "neutralized_quality": _score_neutralized_quality(row.get("retention_industry"), row.get("neutralized_rank_ic_mean"), {"hard_flags": row.get("hard_flags") or []}),
        "incremental_value": _score_incremental_value(candidate, row, failure_dossier, portfolio_contribution, representative_context),
        "deduped_independence": _score_deduped_independence(
            int(row.get("duplicate_peer_count") or 0),
            int(row.get("refinement_peer_count") or 0),
            int(row.get("high_corr_peer_count") or 0),
            int(row.get("crowding_peers") or 0),
        ),
        "split_consistency": _score_split_consistency(int(row.get("split_fail_count") or 0), risk),
        "interpretability": _score_interpretability(candidate.get("name") or row.get("factor_name") or "", {"effective_bucket_label": row.get("effective_bucket_label"), "status": row.get("exposure_status")}, relationships),
    }


def _build_evidence_gate(row: dict[str, Any]) -> dict[str, Any]:
    acceptance_gate = dict(row.get("acceptance_gate") or {})
    status = acceptance_gate.get("status") or "missing"
    if status in _FRONTIER_PASS_STATUSES:
        action = "frontier_ok"
    elif status in _FRONTIER_VALIDATION_STATUSES:
        action = "needs_validation"
    else:
        action = "evidence_missing"
    return {
        "status": status,
        "action": action,
        "explanation": acceptance_gate.get("explanation") or acceptance_gate.get("promotion") or row.get("acceptance_gate_explanation") or "acceptance gate missing or incomplete",
    }



def _build_quality_hard_flags(row: dict[str, Any]) -> dict[str, bool]:
    hard_flag_list = row.get("hard_flags") or []
    retention = row.get("retention_industry")
    neutral_ic = float(row.get("neutralized_rank_ic_mean") or 0.0)
    split_fail_count = int(row.get("split_fail_count") or 0)
    duplicate_peer_count = int(row.get("duplicate_peer_count") or 0)
    refinement_peer_count = int(row.get("refinement_peer_count") or 0)
    high_corr_peer_count = int(row.get("high_corr_peer_count") or 0)
    evidence_gate = row.get("evidence_gate") or {}
    net_metric = row.get("net_metric")
    turnover_daily = row.get("turnover_daily")
    window_count = int(row.get("window_count") or 0)

    failed_60d = False
    if window_count >= 4:
        failed_60d = (
            split_fail_count >= 1
            and (retention is not None and float(retention or 0.0) <= 0.15)
            and neutral_ic <= 0.0
        )

    implementability_weak = False
    if net_metric is not None and float(net_metric) <= 0.0:
        implementability_weak = True
    if turnover_daily is not None and float(turnover_daily) > 0.60:
        implementability_weak = True

    portfolio_contribution = row.get("portfolio_contribution") or {}
    representative_context = row.get("representative_context") or {}
    failure_dossier = row.get("failure_dossier") or {}

    return {
        "failed_60d": failed_60d,
        "neutralized_weak": (retention is not None and float(retention or 0.0) <= 0.15) or neutral_ic < 0.0,
        "duplicate_risk": duplicate_peer_count >= 1 or refinement_peer_count >= 2 or high_corr_peer_count >= 3,
        "untrusted_runs": False,
        "insufficient_window_evidence": window_count < 3,
        "insufficient_long_horizon_evidence": window_count < 4,
        "insufficient_eval_evidence": int(row.get("evaluation_count") or 0) < 40,
        "implementability_weak": implementability_weak,
        "exposure_hard_flag": bool(hard_flag_list),
        "evidence_missing": evidence_gate.get("action") == "evidence_missing",
        "evidence_blocked": evidence_gate.get("status") == "blocked",
        "non_incremental_vs_parent": failure_dossier.get("parent_delta_status") == "non_incremental",
        "negative_portfolio_contribution": float(portfolio_contribution.get("delta_sharpe") or 0.0) < -0.05 or float(portfolio_contribution.get("delta_cost_adjusted_annual_return") or 0.0) < -0.02,
        "representative_suppressed": bool(representative_context) and not representative_context.get("is_representative", True),
    }


def _classify_candidate(total_score: int, hard_flags: dict[str, bool], row: dict[str, Any]) -> str:
    if hard_flags["untrusted_runs"]:
        return "validate-only"
    if hard_flags["duplicate_risk"] or hard_flags["representative_suppressed"]:
        return "duplicate-suppress"
    if hard_flags["evidence_missing"]:
        return "validate-only"
    if hard_flags["evidence_blocked"]:
        return "needs-validation"
    if hard_flags["failed_60d"]:
        if row.get("effective_bucket_label"):
            return "exposure-track"
        return "regime-sensitive"
    if hard_flags["implementability_weak"] or hard_flags["negative_portfolio_contribution"]:
        return "exposure-track" if row.get("effective_bucket_label") else "validate-only"
    if total_score >= 88 and not hard_flags["neutralized_weak"] and not hard_flags["insufficient_long_horizon_evidence"] and not hard_flags["non_incremental_vs_parent"]:
        return "stable-alpha-candidate"
    if total_score >= 72:
        return "needs-validation"
    if total_score >= 50:
        return "exposure-track" if row.get("effective_bucket_label") else "regime-sensitive"
    if total_score >= 30:
        return "validate-only"
    return "drop"


def _promotion_decision(classification: str, hard_flags: dict[str, bool]) -> str:
    if hard_flags["untrusted_runs"]:
        return "hold"
    if hard_flags["duplicate_risk"] or hard_flags["representative_suppressed"]:
        return "suppress"
    if hard_flags["evidence_missing"]:
        return "hold"
    if hard_flags["evidence_blocked"]:
        return "keep_validating"
    if hard_flags["failed_60d"]:
        return "do_not_promote"
    if hard_flags["implementability_weak"] or hard_flags["negative_portfolio_contribution"] or hard_flags["non_incremental_vs_parent"]:
        return "do_not_promote"
    if hard_flags["insufficient_long_horizon_evidence"]:
        return "keep_validating"
    if classification == "stable-alpha-candidate":
        return "promote"
    if classification in {"needs-validation", "exposure-track", "regime-sensitive", "validate-only"}:
        return "keep_validating" if classification == "needs-validation" else "do_not_promote"
    return "do_not_promote"


def _build_quality_summary(classification: str, promotion_decision: str, hard_flags: dict[str, bool], evidence_gate: dict[str, Any]) -> str:
    parts: list[str] = []
    parts.append(_CLASSIFICATION_LABELS.get(classification, classification))
    if hard_flags.get("failed_60d"):
        parts.append("触发 60d 失败硬门槛")
    if hard_flags.get("neutralized_weak"):
        parts.append("neutralized 偏弱")
    if hard_flags.get("duplicate_risk"):
        parts.append("重复/高相关风险")
    if hard_flags.get("insufficient_window_evidence"):
        parts.append("窗口证据不足")
    if hard_flags.get("insufficient_long_horizon_evidence"):
        parts.append("中长窗证据不足")
    if hard_flags.get("implementability_weak"):
        parts.append("implementability 偏弱")
    if hard_flags.get("non_incremental_vs_parent"):
        parts.append("相对父因子缺少新增信息")
    if hard_flags.get("negative_portfolio_contribution"):
        parts.append("组合边际贡献为负")
    if hard_flags.get("representative_suppressed"):
        parts.append("非 representative，先压制近邻变体")
    if hard_flags.get("evidence_missing"):
        parts.append("acceptance gate 缺失")
    elif hard_flags.get("evidence_blocked"):
        parts.append("acceptance gate 阻塞，需先补验证")
    elif evidence_gate.get("status") == "pass":
        parts.append("acceptance gate 通过")
    parts.append(_PROMOTION_LABELS.get(promotion_decision, promotion_decision))
    return "；".join(parts)


def _build_row(
    candidate: dict[str, Any],
    risk_map: dict[str, dict[str, Any]],
    exposure_map: dict[str, dict[str, Any]],
    factor_map: dict[str, dict[str, dict[str, Any]]],
    relationship_map: dict[str, dict[str, Any]],
    thesis_map: dict[str, dict[str, Any]],
    representative_roster: dict[str, dict[str, Any]],
    failure_dossier_map: dict[str, dict[str, Any]],
    portfolio_contribution_map: dict[str, dict[str, Any]],
    approved_universe_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    name = candidate["name"]
    risk = risk_map.get(name) or {}
    exposure = exposure_map.get(name) or {}
    variants = factor_map.get(name) or {}
    raw_row = variants.get("raw_scored") or {}
    neutral_row = variants.get("neutralized") or {}
    relationships = relationship_map.get(name) or {}
    thesis = thesis_map.get(name) or {}
    representative_context = representative_roster.get(name) or {}
    failure_dossier = failure_dossier_map.get(name) or {}
    portfolio_contribution = portfolio_contribution_map.get(name) or {}
    approved_universe_row = approved_universe_map.get(name) or {}
    novelty_payload = load_novelty_judgments(Path(candidate.get("_db_parent") or "."))
    novelty_map = {row.get("candidate_name"): row for row in (novelty_payload.get("rows") or []) if row.get("candidate_name")}
    novelty_row = novelty_map.get(name) or {}

    duplicate_peers = relationships.get("duplicate_peers") or []
    refinement_peers = relationships.get("refinement_peers") or []
    high_corr_peers = relationships.get("high_corr_peers") or []

    raw_ic = raw_row.get("rank_ic_mean")
    neutral_ic = neutral_row.get("rank_ic_mean")
    retention = exposure.get("retention_industry")
    if retention is None and raw_ic not in (None, 0):
        retention = float(neutral_ic or 0.0) / float(raw_ic)

    recent_strength = _clip((float(candidate.get("latest_recent_final_score") or candidate.get("latest_final_score") or 0.0) + 1.0) / 10.5)
    window_support = _clip(float(candidate.get("window_count") or 0.0) / 4.0)
    pass_support = _clip(float(candidate.get("pass_rate") or 0.0))
    robustness_support = _clip(float(risk.get("robustness_score") or 0.0))
    retention_support = 0.45 if retention is None else _clip((float(retention or 0.0) + 0.1) / 0.5)
    risk_penalty = _clip(float(risk.get("risk_score") or 100.0) / 100.0)
    crowding_signal = max(
        float(exposure.get("crowding_peers") or 0.0),
        float(len(high_corr_peers)),
        float(len(duplicate_peers)),
    )
    crowding_penalty = _clip(crowding_signal / 3.0)
    split_penalty = _clip(float(exposure.get("split_fail_count") or raw_row.get("split_fail_count") or 0.0) / 2.0)

    promotion_score = round(
        (
            recent_strength * 34
            + window_support * 18
            + pass_support * 18
            + robustness_support * 16
            + retention_support * 14
            - risk_penalty * 12
            - crowding_penalty * 8
            - split_penalty * 10
        ),
        6,
    )

    row = {
        "factor_name": name,
        "family": candidate.get("family") or "other",
        "candidate_status": candidate.get("status"),
        "latest_final_score": candidate.get("latest_final_score"),
        "latest_recent_final_score": candidate.get("latest_recent_final_score"),
        "avg_final_score": candidate.get("avg_final_score"),
        "pass_rate": candidate.get("pass_rate"),
        "evaluation_count": candidate.get("evaluation_count"),
        "window_count": candidate.get("window_count"),
        "next_action": candidate.get("next_action"),
        "risk_level": risk.get("risk_level"),
        "risk_score": risk.get("risk_score"),
        "robustness_score": risk.get("robustness_score"),
        "passing_check_count": risk.get("passing_check_count"),
        "failing_check_count": risk.get("failing_check_count"),
        "raw_rank_ic_mean": raw_ic,
        "raw_rank_ic_ir": raw_row.get("rank_ic_ir"),
        "neutralized_rank_ic_mean": neutral_ic,
        "retention_industry": retention,
        "split_fail_count": exposure.get("split_fail_count", raw_row.get("split_fail_count")),
        "crowding_peers": max(
            int(exposure.get("crowding_peers") or 0),
            len(raw_row.get("high_corr_peers") or []),
            len(high_corr_peers),
        ),
        "duplicate_peer_count": len(duplicate_peers),
        "refinement_peer_count": len(refinement_peers),
        "high_corr_peer_count": len(high_corr_peers),
        "duplicate_peers": [rel.get("name") for rel in duplicate_peers[:4]],
        "refinement_peers": [rel.get("name") for rel in refinement_peers[:4]],
        "exposure_total_score": exposure.get("total_score"),
        "exposure_status": exposure.get("status"),
        "recommended_max_weight": exposure.get("recommended_max_weight"),
        "effective_bucket_label": exposure.get("effective_bucket_label"),
        "turnover_daily": exposure.get("turnover_daily"),
        "net_metric": exposure.get("net_metric"),
        "hard_flags": exposure.get("hard_flags") or [],
        "promotion_score": promotion_score,
        "acceptance_gate": risk.get("acceptance_gate") or {},
        "acceptance_gate_explanation": risk.get("acceptance_gate_explanation"),
        "thesis_id": thesis.get("thesis_id"),
        "thesis_title": thesis.get("title"),
        "thesis_type": thesis.get("thesis_type"),
        "institutional_bucket_key": thesis.get("institutional_bucket_key"),
        "institutional_bucket_label": thesis.get("institutional_bucket_label"),
        "thesis_text": thesis.get("thesis_text"),
        "thesis_status": thesis.get("status"),
        "thesis_invalidation": thesis.get("invalidation") or [],
        "representative_context": representative_context,
        "representative_primary_candidate": representative_context.get("primary_candidate") or thesis.get("representative_candidate"),
        "representative_rank": representative_context.get("representative_rank") or thesis.get("representative_rank"),
        "representative_count": representative_context.get("representative_count") or thesis.get("representative_count"),
        "is_representative": representative_context.get("is_representative"),
        "is_primary_representative": representative_context.get("is_primary_representative"),
        "failure_dossier": failure_dossier,
        "failure_modes": failure_dossier.get("failure_modes") or [],
        "failure_recommended_action": failure_dossier.get("recommended_action"),
        "failure_regime_dependency": failure_dossier.get("regime_dependency"),
        "portfolio_contribution": portfolio_contribution,
        "approved_universe_member": bool(approved_universe_row),
        "approved_universe_reason": approved_universe_row.get("approved_reason"),
        "approved_universe_version": approved_universe_row.get("selection_policy_version"),
        "approved_universe_source_windows": approved_universe_row.get("source_windows") or [],
        "approved_universe_state": approved_universe_row.get("lifecycle_state") or approved_universe_row.get("universe_state"),
        "approved_universe_governance_action": approved_universe_row.get("governance_action"),
        "approved_universe_allocated_weight": approved_universe_row.get("allocated_weight") or approved_universe_row.get("portfolio_weight_hint"),
        "approved_universe_max_weight": approved_universe_row.get("max_weight"),
        "approved_universe_budget_reason": approved_universe_row.get("budget_reason"),
        "novelty_class": novelty_row.get("novelty_class"),
        "incrementality_confidence": novelty_row.get("incrementality_confidence"),
        "novelty_recommended_action": novelty_row.get("recommended_action"),
        "novelty_reasoning_summary": novelty_row.get("reasoning_summary"),
    }
    decision_key, reasons = _decide_candidate(row)
    row["decision_key"] = decision_key
    row["decision_label"] = _DECISION_LABELS[decision_key]
    row["decision_reasons"] = reasons[:4]
    row["decision_summary"] = "；".join(reasons[:3]) if reasons else _DECISION_LABELS[decision_key]
    row["decision_priority"] = _DECISION_PRIORITY[decision_key]

    quality_scores = _build_quality_scores(candidate, risk, row, relationships, failure_dossier, portfolio_contribution, representative_context)
    total_quality_score = int(sum(quality_scores.values()))
    row["evidence_gate"] = _build_evidence_gate(row)
    quality_hard_flags = _build_quality_hard_flags(row)
    quality_classification = _classify_candidate(total_quality_score, quality_hard_flags, row)
    quality_promotion_decision = _promotion_decision(quality_classification, quality_hard_flags)

    row["quality_scores"] = quality_scores
    row["quality_total_score"] = total_quality_score
    row["quality_hard_flags"] = quality_hard_flags
    row["quality_classification"] = quality_classification
    row["quality_classification_label"] = _CLASSIFICATION_LABELS.get(quality_classification, quality_classification)
    row["quality_promotion_decision"] = quality_promotion_decision
    row["quality_promotion_decision_label"] = _PROMOTION_LABELS.get(quality_promotion_decision, quality_promotion_decision)
    row["quality_summary"] = _build_quality_summary(quality_classification, quality_promotion_decision, quality_hard_flags, row["evidence_gate"])
    row["scorecard_schema_version"] = "factor-quality-v2"
    return row


def build_promotion_scorecard(db_path: str | Path, limit: int = 12) -> dict[str, Any]:
    db_path = Path(db_path)
    store = ExperimentStore(db_path)
    conn = store.conn
    conn.row_factory = sqlite3.Row
    try:
        latest_run = _latest_finished_run(conn)
        if not latest_run:
            return {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "latest_run": None,
                "rows": [],
                "summary": {"has_data": False},
                "rubric": {"version": "factor-quality-v2"},
            }

        candidates = [
            dict(row)
            for row in conn.execute(
                """
                SELECT name, family, status, evaluation_count, window_count,
                       avg_final_score, best_final_score, latest_final_score, latest_recent_final_score,
                       pass_rate, next_action
                FROM factor_candidates
                WHERE latest_final_score IS NOT NULL OR latest_recent_final_score IS NOT NULL
                ORDER BY COALESCE(latest_recent_final_score, latest_final_score, -999) DESC, evaluation_count DESC, name ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        ]
        risk_map = _load_risk_map(conn)
        exposure_map = _load_latest_exposure_map(conn, latest_run["run_id"])
        factor_map = _load_latest_factor_map(conn, latest_run["run_id"])
        relationship_map = _load_relationship_map(conn)
        thesis_map = _load_thesis_map(conn)
        representative_roster = _load_representative_roster(latest_run.get("output_dir"))
        portfolio_contribution_map = _load_portfolio_contribution_map(db_path.parent)
        approved_universe_map = _load_approved_universe_map(db_path.parent)
        all_candidates = store.list_factor_candidates(limit=2000)
        all_evaluations = store.list_factor_evaluations(limit=5000)
        all_relationships = store.list_candidate_relationships(limit=5000)
        failure_dossier_rows = build_candidate_failure_dossiers(
            all_candidates,
            all_evaluations,
            all_relationships,
            focus_names=[row.get("name") for row in candidates if row.get("name")],
            limit=len(candidates),
        )
        failure_dossier_map = {
            row.get("candidate_name"): row
            for row in failure_dossier_rows
            if row.get("candidate_name")
        }

        rows = [
            _build_row(
                {**row, "_db_parent": str(db_path.parent)},
                risk_map,
                exposure_map,
                factor_map,
                relationship_map,
                thesis_map,
                representative_roster,
                failure_dossier_map,
                portfolio_contribution_map,
                approved_universe_map,
            )
            for row in candidates
        ]
        rows.sort(
            key=lambda row: (
                _CLASSIFICATION_PRIORITY.get(row.get("quality_classification") or "drop", 99),
                -int(row.get("quality_total_score") or 0),
                row["decision_priority"],
                -float(row.get("promotion_score") or 0.0),
                -float(row.get("latest_recent_final_score") or row.get("latest_final_score") or 0.0),
                row.get("factor_name") or "",
            )
        )

        summary = {
            "has_data": bool(rows),
            "core_candidate_count": len([row for row in rows if row["decision_key"] == "core_candidate"]),
            "validate_now_count": len([row for row in rows if row["decision_key"] == "validate_now"]),
            "dedupe_first_count": len([row for row in rows if row["decision_key"] == "dedupe_first"]),
            "regime_sensitive_count": len([row for row in rows if row["decision_key"] == "regime_sensitive"]),
            "watchlist_count": len([row for row in rows if row["decision_key"] == "watchlist"]),
            "drop_count": len([row for row in rows if row["decision_key"] == "drop_from_frontier"]),
            "stable_alpha_candidate_count": len([row for row in rows if row["quality_classification"] == "stable-alpha-candidate"]),
            "needs_validation_count": len([row for row in rows if row["quality_classification"] == "needs-validation"]),
            "exposure_track_count": len([row for row in rows if row["quality_classification"] == "exposure-track"]),
            "quality_regime_sensitive_count": len([row for row in rows if row["quality_classification"] == "regime-sensitive"]),
            "duplicate_suppress_count": len([row for row in rows if row["quality_classification"] == "duplicate-suppress"]),
            "validate_only_count": len([row for row in rows if row["quality_classification"] == "validate-only"]),
            "quality_drop_count": len([row for row in rows if row["quality_classification"] == "drop"]),
            "positive_portfolio_contributor_count": len([row for row in rows if (row.get("portfolio_contribution") or {}).get("contribution_class") == "positive"]),
            "negative_portfolio_contributor_count": len([row for row in rows if (row.get("portfolio_contribution") or {}).get("contribution_class") == "negative"]),
            "representative_candidate_count": len([row for row in rows if row.get("is_representative")]),
            "approved_universe_member_count": len([row for row in rows if row.get("approved_universe_member")]),
            "approved_universe_state_counts": dict(Counter(row.get("approved_universe_state") or "outside" for row in rows)),
        }
        priority_rows = [
            {
                "factor_name": row["factor_name"],
                "decision_label": row["decision_label"],
                "decision_summary": row["decision_summary"],
                "quality_classification": row["quality_classification"],
                "quality_classification_label": row["quality_classification_label"],
                "quality_summary": row["quality_summary"],
            }
            for row in rows[:3]
        ]
        summary["priority_rows"] = priority_rows

        rubric = {
            "version": "factor-quality-v2",
            "dimensions": {
                "cross_window_robustness": {"weight": 30, "description": "跨窗口稳健性"},
                "neutralized_quality": {"weight": 20, "description": "neutralized 保留质量"},
                "incremental_value": {"weight": 20, "description": "新增信息 / 增量价值"},
                "deduped_independence": {"weight": 15, "description": "去重后独立性"},
                "split_consistency": {"weight": 10, "description": "split 一致性"},
                "interpretability": {"weight": 5, "description": "可解释性"},
            },
            "hard_flags": {
                "failed_60d": "触发 60d 失败硬门槛（当前为基于现有证据的近似判定）",
                "neutralized_weak": "neutralized 长期偏弱或 retention 太薄",
                "duplicate_risk": "重复/高相关风险高，不应独立记功",
                "untrusted_runs": "结果来源不可信时暂停晋升",
                "insufficient_window_evidence": "跨窗口证据不足",
                "insufficient_eval_evidence": "评估次数不足",
                "exposure_hard_flag": "Exposure scorecard 存在硬标志",
                "evidence_missing": "acceptance gate 缺失，不能把高分当成高质量因子",
                "evidence_blocked": "acceptance gate 阻塞，需先补验证再晋升",
                "non_incremental_vs_parent": "相对父因子没有足够新增信息",
                "negative_portfolio_contribution": "leave-one-out 组合边际贡献为负",
                "representative_suppressed": "当前候选不是簇内 representative，先压制近邻变体",
            },
            "classifications": _CLASSIFICATION_LABELS,
            "promotion_decisions": _PROMOTION_LABELS,
        }

        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "latest_run": latest_run,
            "rows": rows,
            "summary": summary,
            "rubric": rubric,
        }
    finally:
        conn.close()


def write_promotion_scorecard(db_path: str | Path, output_path: str | Path, limit: int = 12) -> dict[str, Any]:
    payload = build_promotion_scorecard(db_path=db_path, limit=limit)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
