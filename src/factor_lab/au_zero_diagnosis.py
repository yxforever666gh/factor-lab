from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _latest_run_status_rows(db_path: Path) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        run = conn.execute(
            """
            SELECT run_id, created_at_utc, config_path, output_dir
            FROM workflow_runs
            WHERE status='finished'
            ORDER BY created_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
        if not run:
            return None, []
        run_d = dict(run)
        output_dir = Path(run_d.get("output_dir") or "")
        rows = _read_json(output_dir / "candidate_status_snapshot.json", [])
        return run_d, rows if isinstance(rows, list) else []
    finally:
        conn.close()


def build_au_zero_diagnosis(db_path: str | Path, artifacts_dir: str | Path) -> dict[str, Any]:
    db_path = Path(db_path)
    artifacts_dir = Path(artifacts_dir)
    latest_run, status_rows = _latest_run_status_rows(db_path)
    au = _read_json(artifacts_dir / "approved_candidate_universe.json", {})
    au_debug = _read_json(artifacts_dir / "approved_candidate_universe_debug.json", {})
    promotion = _read_json(artifacts_dir / "promotion_scorecard.json", {})
    novelty = _read_json(artifacts_dir / "novelty_judgments.json", {})
    governance = _read_json(artifacts_dir / "approved_candidate_universe_governance.json", {})
    contribution = _read_json(artifacts_dir / "paper_portfolio" / "portfolio_contribution_report.json", {})

    debug_map = {row.get("factor_name"): row for row in (au_debug.get("rows") or []) if row.get("factor_name")}
    promotion_map = {row.get("factor_name"): row for row in (promotion.get("rows") or []) if row.get("factor_name")}
    novelty_map = {row.get("candidate_name"): row for row in (novelty.get("rows") or []) if row.get("candidate_name")}
    governance_map = {row.get("factor_name"): row for row in (governance.get("rows") or []) if row.get("factor_name")}
    contribution_map = {row.get("factor_name"): row for row in (contribution.get("rows") or []) if row.get("factor_name")}
    au_names = {row.get("factor_name") for row in (au.get("rows") or []) if row.get("factor_name")}

    candidates = [row for row in status_rows if row.get("research_stage") == "candidate"]
    watchlist = [row for row in status_rows if row.get("research_stage") == "watchlist"]
    focus_names = []
    for row in candidates + watchlist:
        name = row.get("factor_name")
        if name and name not in focus_names:
            focus_names.append(name)
    # Also include all AU-debug-approved candidates because governance can demote them after approval.
    for row in (au_debug.get("rows") or []):
        name = row.get("factor_name")
        if name and row.get("approved") and name not in focus_names:
            focus_names.append(name)

    rows: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    verdict_counts: Counter[str] = Counter()
    for name in focus_names:
        status = next((row for row in status_rows if row.get("factor_name") == name), {})
        debug = debug_map.get(name) or {}
        promo = promotion_map.get(name) or {}
        nov = novelty_map.get(name) or {}
        gov = governance_map.get(name) or {}
        contrib = contribution_map.get(name) or {}
        rejection_reasons = list(debug.get("rejection_reasons") or [])
        governance_action = gov.get("governance_action") or debug.get("governance_action")
        if governance_action in {"demote_candidate", "demote_bridge_candidate"} and "governance_demotion" not in rejection_reasons:
            rejection_reasons.append("governance_demotion")
        for reason in rejection_reasons or ["not_in_debug"]:
            reason_counts[reason] += 1

        novelty_class = nov.get("novelty_class")
        contribution_class = contrib.get("contribution_class") or promo.get("portfolio_contribution", {}).get("contribution_class")
        hard_flags = promo.get("quality_hard_flags") or {}
        verdict = "valid_rejection"
        if name in au_names:
            verdict = "approved"
        elif not debug and status:
            verdict = "artifact_stale_or_inconsistent"
        elif rejection_reasons == ["governance_demotion"] and contribution_class in {None, "neutral"}:
            verdict = "possible_false_negative"
        elif novelty_class in {"meaningful_extension", "new_mechanism", "new_mechanism_low_evidence", "meaningful_extension_low_confidence"} and "non_incremental_vs_parent" not in rejection_reasons:
            verdict = "possible_false_negative"
        elif not rejection_reasons and status.get("research_stage") == "candidate":
            verdict = "artifact_stale_or_inconsistent"
        elif hard_flags.get("evidence_missing") and status.get("research_stage") == "candidate":
            verdict = "possible_false_negative"
        verdict_counts[verdict] += 1
        rows.append(
            {
                "candidate_name": name,
                "latest_stage": status.get("research_stage"),
                "latest_window": latest_run.get("config_path") if latest_run else None,
                "au_member": name in au_names,
                "au_approved_flag": debug.get("approved"),
                "rejection_reasons": rejection_reasons,
                "quality_classification": promo.get("quality_classification"),
                "quality_promotion_decision": promo.get("quality_promotion_decision"),
                "quality_hard_flags": hard_flags,
                "novelty_class": novelty_class,
                "novelty_action": nov.get("recommended_action"),
                "governance_action": governance_action,
                "negative_contribution_streak": gov.get("negative_contribution_streak") or debug.get("negative_contribution_streak"),
                "portfolio_contribution_class": contribution_class,
                "verdict": verdict,
            }
        )

    summary = {
        "latest_run": latest_run,
        "au_count": len(au_names),
        "latest_candidate_count": len(candidates),
        "latest_watchlist_count": len(watchlist),
        "diagnosed_count": len(rows),
        "rejection_reason_counts": dict(reason_counts),
        "verdict_counts": dict(verdict_counts),
    }
    if len(au_names) == 0:
        top_reason = reason_counts.most_common(1)[0][0] if reason_counts else "no_candidate_or_debug_rows"
        direct_cause = f"AU=0 direct cause: {top_reason}"
    else:
        direct_cause = f"AU is not zero: approved_count={len(au_names)}"
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": "factor_lab.au_zero_diagnosis.v1",
        "summary": {**summary, "direct_cause": direct_cause},
        "rows": rows,
    }


def write_au_zero_diagnosis(db_path: str | Path, artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    payload = build_au_zero_diagnosis(db_path, artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "au_zero_diagnosis.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# AU=0 诊断", "", f"- 生成时间：{payload['generated_at_utc']}", f"- 直接结论：{payload['summary'].get('direct_cause')}", f"- AU 数量：{payload['summary'].get('au_count')}", f"- 最新 candidate 数：{payload['summary'].get('latest_candidate_count')}", "", "## 候选拒绝链"]
    for row in payload.get("rows") or []:
        lines.append(f"- {row.get('candidate_name')}: stage={row.get('latest_stage')}, verdict={row.get('verdict')}, reasons={row.get('rejection_reasons')}, novelty={row.get('novelty_class')}, governance={row.get('governance_action')}, contribution={row.get('portfolio_contribution_class')}")
    (artifacts_dir / "au_zero_diagnosis.md").write_text("\n".join(lines), encoding="utf-8")
    return payload
