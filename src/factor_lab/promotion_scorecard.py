from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.storage import ExperimentStore


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
        SELECT run_id, created_at_utc, config_path
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
               crowding_peers, recommended_max_weight, effective_bucket_label,
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


def _score_incremental_value(candidate: dict[str, Any], row: dict[str, Any]) -> int:
    latest_score = float(candidate.get("latest_recent_final_score") or candidate.get("latest_final_score") or 0.0)
    avg_score = float(candidate.get("avg_final_score") or 0.0)
    pass_rate = float(candidate.get("pass_rate") or 0.0)
    exposure_total = float(row.get("exposure_total_score") or 0.0)

    score = 0.0
    score += _clip((latest_score - 5.0) / 4.5) * 8.0
    score += _clip((avg_score - 1.5) / 5.0) * 4.0
    score += _clip(pass_rate / 0.5) * 4.0
    score += _clip(exposure_total / 80.0) * 4.0
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


def _build_quality_scores(candidate: dict[str, Any], risk: dict[str, Any], row: dict[str, Any], relationships: dict[str, Any]) -> dict[str, int]:
    return {
        "cross_window_robustness": _score_cross_window(candidate, risk, int(row.get("split_fail_count") or 0)),
        "neutralized_quality": _score_neutralized_quality(row.get("retention_industry"), row.get("neutralized_rank_ic_mean"), {"hard_flags": row.get("hard_flags") or []}),
        "incremental_value": _score_incremental_value(candidate, row),
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

    failed_60d = False
    if row.get("window_count") and int(row.get("window_count") or 0) >= 4:
        failed_60d = (
            split_fail_count >= 1
            and (retention is not None and float(retention or 0.0) <= 0.15)
            and neutral_ic <= 0.0
        )

    return {
        "failed_60d": failed_60d,
        "neutralized_weak": (retention is not None and float(retention or 0.0) <= 0.15) or neutral_ic < 0.0,
        "duplicate_risk": duplicate_peer_count >= 1 or refinement_peer_count >= 2 or high_corr_peer_count >= 3,
        "untrusted_runs": False,
        "insufficient_window_evidence": int(row.get("window_count") or 0) < 3,
        "insufficient_eval_evidence": int(row.get("evaluation_count") or 0) < 40,
        "exposure_hard_flag": bool(hard_flag_list),
        "evidence_missing": evidence_gate.get("action") == "evidence_missing",
        "evidence_blocked": evidence_gate.get("status") == "blocked",
    }


def _classify_candidate(total_score: int, hard_flags: dict[str, bool], row: dict[str, Any]) -> str:
    if hard_flags["untrusted_runs"]:
        return "validate-only"
    if hard_flags["duplicate_risk"]:
        return "duplicate-suppress"
    if hard_flags["evidence_missing"]:
        return "validate-only"
    if hard_flags["evidence_blocked"]:
        return "needs-validation"
    if hard_flags["failed_60d"]:
        if row.get("effective_bucket_label"):
            return "exposure-track"
        return "regime-sensitive"
    if total_score >= 85 and not hard_flags["neutralized_weak"] and not hard_flags["insufficient_window_evidence"]:
        return "stable-alpha-candidate"
    if total_score >= 70:
        return "needs-validation"
    if total_score >= 50:
        return "exposure-track" if row.get("effective_bucket_label") else "regime-sensitive"
    if total_score >= 30:
        return "validate-only"
    return "drop"


def _promotion_decision(classification: str, hard_flags: dict[str, bool]) -> str:
    if hard_flags["untrusted_runs"]:
        return "hold"
    if hard_flags["duplicate_risk"]:
        return "suppress"
    if hard_flags["evidence_missing"]:
        return "hold"
    if hard_flags["evidence_blocked"]:
        return "keep_validating"
    if hard_flags["failed_60d"]:
        return "do_not_promote"
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
) -> dict[str, Any]:
    name = candidate["name"]
    risk = risk_map.get(name) or {}
    exposure = exposure_map.get(name) or {}
    variants = factor_map.get(name) or {}
    raw_row = variants.get("raw_scored") or {}
    neutral_row = variants.get("neutralized") or {}
    relationships = relationship_map.get(name) or {}

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
    }
    decision_key, reasons = _decide_candidate(row)
    row["decision_key"] = decision_key
    row["decision_label"] = _DECISION_LABELS[decision_key]
    row["decision_reasons"] = reasons[:4]
    row["decision_summary"] = "；".join(reasons[:3]) if reasons else _DECISION_LABELS[decision_key]
    row["decision_priority"] = _DECISION_PRIORITY[decision_key]

    quality_scores = _build_quality_scores(candidate, risk, row, relationships)
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
    row["scorecard_schema_version"] = "factor-quality-v1"
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
                "rubric": {"version": "factor-quality-v1"},
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

        rows = [_build_row(row, risk_map, exposure_map, factor_map, relationship_map) for row in candidates]
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
            "version": "factor-quality-v1",
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
