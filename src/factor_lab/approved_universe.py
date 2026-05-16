from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.storage import ExperimentStore


SELECTION_POLICY_VERSION = "approved-universe-v2"


def _window_label_from_run(run: dict[str, Any]) -> str:
    config_name = Path(run.get("config_path") or "").name.lower()
    output_name = Path(run.get("output_dir") or "").name.lower()
    text = f"{config_name} {output_name}"
    if "recent_45d" in text:
        return "recent_45d"
    if "recent_90d" in text:
        return "recent_90d"
    if "recent_120d" in text:
        return "recent_120d"
    if "30d_back" in text:
        return "rolling_30d_back"
    if "60d_back" in text:
        return "rolling_60d_back"
    if "120d_back" in text:
        return "rolling_120d_back"
    if "expanding" in text:
        return "expanding"
    return Path(run.get("config_path") or run.get("output_dir") or "unknown").stem


def resolve_recent_finished_runs(db_path: str | Path, limit: int = 12) -> list[dict[str, Any]]:
    db_path = Path(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT run_id, created_at_utc, output_dir, config_path
            FROM workflow_runs
            WHERE status = 'finished'
              AND COALESCE(config_path, '') NOT LIKE 'artifacts/generated_ab_configs/%'
              AND COALESCE(output_dir, '') NOT LIKE 'artifacts/ab_harness/%'
              AND COALESCE(config_path, '') NOT LIKE 'artifacts/generated_candidate_configs/%'
              AND COALESCE(output_dir, '') NOT LIKE 'artifacts/generated_candidate_runs/%'
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def load_run_candidate_artifacts(run: dict[str, Any]) -> dict[str, Any]:
    output_dir = Path(run.get("output_dir") or "")

    def read_json(name: str, default: Any) -> Any:
        path = output_dir / name
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    candidate_pool = read_json("candidate_pool.json", [])
    candidate_status_snapshot = read_json("candidate_status_snapshot.json", [])
    cluster_representatives = read_json("cluster_representatives.json", [])
    return {
        "run": run,
        "window_label": _window_label_from_run(run),
        "candidate_pool": candidate_pool if isinstance(candidate_pool, list) else [],
        "candidate_status_snapshot": candidate_status_snapshot if isinstance(candidate_status_snapshot, list) else [],
        "cluster_representatives": cluster_representatives if isinstance(cluster_representatives, list) else [],
        "dataset_path": output_dir / "dataset.csv",
    }


def _load_recent_run_artifacts(db_path: str | Path, limit: int = 12) -> list[dict[str, Any]]:
    return [load_run_candidate_artifacts(run) for run in resolve_recent_finished_runs(db_path, limit=limit)]


def _scorecard_map(db_path: str | Path, limit: int = 2000) -> dict[str, dict[str, Any]]:
    payload = build_promotion_scorecard(db_path=db_path, limit=limit)
    return {row.get("factor_name"): row for row in (payload.get("rows") or []) if row.get("factor_name")}


def _expression_for(name: str, candidate_map: dict[str, dict[str, Any]], pools: list[dict[str, Any]], reps: list[dict[str, Any]]) -> str | None:
    for row in pools:
        if row.get("factor_name") == name and row.get("expression"):
            return row.get("expression")
    for row in reps:
        if row.get("factor_name") == name and row.get("expression"):
            return row.get("expression")
    candidate = candidate_map.get(name) or {}
    return candidate.get("expression") or (candidate.get("definition") or {}).get("expression")


def _aggregate_candidate_state(
    db_path: str | Path,
    recent_artifacts: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    store = ExperimentStore(db_path)
    candidates = store.list_factor_candidates(limit=5000)
    candidate_map = {row.get("name"): row for row in candidates if row.get("name")}
    scorecard_map = _scorecard_map(db_path, limit=5000)

    aggregated: dict[str, dict[str, Any]] = {}
    for artifact in recent_artifacts:
        window_label = artifact["window_label"]
        candidate_pool = artifact["candidate_pool"]
        candidate_pool_names = {row.get("factor_name") for row in candidate_pool if row.get("factor_name")}
        status_rows = artifact["candidate_status_snapshot"]
        reps = artifact["cluster_representatives"]
        rep_map = {row.get("factor_name"): row for row in reps if row.get("factor_name")}

        for row in status_rows:
            name = row.get("factor_name")
            if not name:
                continue
            bucket = aggregated.setdefault(
                name,
                {
                    "factor_name": name,
                    "expression": None,
                    "family": (candidate_map.get(name) or {}).get("family"),
                    "factor_role": row.get("factor_role") or (candidate_map.get(name) or {}).get("factor_role"),
                    "source_run_ids": [],
                    "source_windows": [],
                    "candidate_pool_count": 0,
                    "watchlist_count": 0,
                    "graveyard_count": 0,
                    "explore_count": 0,
                    "raw_pass_count": 0,
                    "neutralized_pass_count": 0,
                    "rolling_pass_count": 0,
                    "cross_window_support_count": 0,
                    "long_window_support_count": 0,
                    "recent_window_support_count": 0,
                    "primary_representative_hits": 0,
                    "representative_hits": 0,
                    "blocking_reasons": [],
                    "promotion_reasons": [],
                    "latest_stage": None,
                    "latest_window": None,
                    "candidate_row": candidate_map.get(name) or {},
                    "scorecard_row": scorecard_map.get(name) or {},
                    "latest_rep_row": {},
                },
            )
            bucket["expression"] = bucket.get("expression") or _expression_for(name, candidate_map, candidate_pool, reps)
            if artifact["run"]["run_id"] not in bucket["source_run_ids"]:
                bucket["source_run_ids"].append(artifact["run"]["run_id"])
            if window_label not in bucket["source_windows"]:
                bucket["source_windows"].append(window_label)

            stage = row.get("research_stage") or "explore"
            if name in candidate_pool_names or stage == "candidate":
                bucket["candidate_pool_count"] += 1
            elif stage == "watchlist":
                bucket["watchlist_count"] += 1
            elif stage == "graveyard":
                bucket["graveyard_count"] += 1
            else:
                bucket["explore_count"] += 1

            raw_pass = bool(row.get("raw_pass"))
            neutralized_pass = bool(row.get("neutralized_pass"))
            rolling_pass = bool(row.get("rolling_pass"))
            if raw_pass:
                bucket["raw_pass_count"] += 1
            if neutralized_pass:
                bucket["neutralized_pass_count"] += 1
            if rolling_pass:
                bucket["rolling_pass_count"] += 1
            if raw_pass or neutralized_pass or rolling_pass:
                bucket["cross_window_support_count"] += 1
                if window_label.startswith("recent_"):
                    bucket["recent_window_support_count"] += 1
                else:
                    bucket["long_window_support_count"] += 1

            rep_row = rep_map.get(name) or {}
            if rep_row:
                bucket["representative_hits"] += 1
                bucket["latest_rep_row"] = rep_row
                if rep_row.get("is_primary_representative"):
                    bucket["primary_representative_hits"] += 1

            for reason in row.get("blocking_reasons") or []:
                if reason not in bucket["blocking_reasons"]:
                    bucket["blocking_reasons"].append(reason)
            if row.get("promotion_reason") and row.get("promotion_reason") not in bucket["promotion_reasons"]:
                bucket["promotion_reasons"].append(row.get("promotion_reason"))

            bucket["latest_stage"] = stage
            bucket["latest_window"] = window_label

    return aggregated


def _approved_or_rejected(bucket: dict[str, Any]) -> tuple[bool, str, list[str]]:
    scorecard = bucket.get("scorecard_row") or {}
    hard_flags = scorecard.get("quality_hard_flags") or {}

    rejection_reasons: list[str] = []

    is_primary_rep = bucket.get("primary_representative_hits", 0) >= 1
    candidate_count = int(bucket.get("candidate_pool_count") or 0)
    watchlist_count = int(bucket.get("watchlist_count") or 0)
    long_support_count = int(bucket.get("long_window_support_count") or 0)
    recent_support_count = int(bucket.get("recent_window_support_count") or 0)
    graveyard_count = int(bucket.get("graveyard_count") or 0)

    short_only = candidate_count >= 1 and long_support_count == 0 and recent_support_count >= 1 and graveyard_count >= 1
    if short_only:
        rejection_reasons.append("short_only_recent_candidate")

    # Do not hard-reject on leave-one-out contribution at universe-construction time.
    # For tiny universes (especially 1-factor cases), leave-one-out is structurally negative
    # and would collapse the approved universe back to empty. Keep it as metadata for later
    # planner / promotion integration instead of using it as a universe admission hard fail.
    if hard_flags.get("non_incremental_vs_parent"):
        rejection_reasons.append("non_incremental_vs_parent")
    if hard_flags.get("representative_suppressed"):
        rejection_reasons.append("representative_suppressed")
    if (bucket.get("scorecard_row") or {}).get("quality_classification") in {"drop"}:
        rejection_reasons.append("scorecard_rejected")

    if candidate_count >= 1 and long_support_count >= 1 and not rejection_reasons:
        return True, "candidate_pool_supported_by_longer_windows", []

    if (
        watchlist_count >= 1
        and is_primary_rep
        and long_support_count >= 1
        and bucket.get("raw_pass_count", 0) >= 1
        and not rejection_reasons
    ):
        return True, "primary_representative_watchlist_with_cross_window_support", []

    if (
        is_primary_rep
        and (scorecard.get("quality_classification") in {"stable-alpha-candidate", "needs-validation"})
        and int(scorecard.get("quality_total_score") or 0) >= 72
        and not hard_flags.get("evidence_missing")
        and not hard_flags.get("implementability_weak")
        and not rejection_reasons
    ):
        return True, "scorecard_quality_gate", []

    if (
        is_primary_rep
        and watchlist_count >= 1
        and recent_support_count >= 1
        and long_support_count >= 1
        and "high_corr:" not in " ".join(bucket.get("blocking_reasons") or [])
        and not rejection_reasons
    ):
        return True, "representative_watchlist_bridge", []

    if not rejection_reasons:
        if candidate_count == 0 and watchlist_count == 0:
            rejection_reasons.append("no_candidate_or_watchlist_support")
        elif not is_primary_rep:
            rejection_reasons.append("not_primary_representative")
        elif long_support_count == 0:
            rejection_reasons.append("no_long_window_support")
        else:
            rejection_reasons.append("did_not_meet_approval_rules")

    return False, "", rejection_reasons


def _bridge_candidate(bucket: dict[str, Any]) -> tuple[bool, str]:
    scorecard = bucket.get("scorecard_row") or {}
    hard_flags = scorecard.get("quality_hard_flags") or {}
    is_primary_rep = bucket.get("primary_representative_hits", 0) >= 1
    candidate_count = int(bucket.get("candidate_pool_count") or 0)
    recent_support_count = int(bucket.get("recent_window_support_count") or 0)
    long_support_count = int(bucket.get("long_window_support_count") or 0)
    graveyard_count = int(bucket.get("graveyard_count") or 0)
    institutional_bucket_label = scorecard.get("institutional_bucket_label") or scorecard.get("effective_bucket_label") or ""

    if (
        is_primary_rep
        and candidate_count >= 1
        and recent_support_count >= 1
        and long_support_count == 0
        and graveyard_count >= 1
        and not hard_flags.get("non_incremental_vs_parent")
        and not hard_flags.get("representative_suppressed")
    ):
        return True, "bridge_recent_probe"

    if (
        is_primary_rep
        and bucket.get("watchlist_count", 0) >= 1
        and long_support_count >= 1
        and "Controlled Composite" in institutional_bucket_label
        and int(scorecard.get("quality_total_score") or 0) >= 40
        and not hard_flags.get("non_incremental_vs_parent")
    ):
        return True, "bridge_controlled_composite"

    return False, ""


def _approval_profile(row: dict[str, Any]) -> dict[str, Any]:
    reason = row.get("approved_reason") or ""
    bucket_label = row.get("institutional_bucket_label") or ""
    profile = {
        "approval_tier": "core",
        "portfolio_bucket": "core_alpha",
        "portfolio_bucket_label": "Core Alpha",
        "portfolio_weight_hint": 0.5,
        "portfolio_weight_target": 0.5,
        "max_weight": 0.4,
        "family_budget_cap": 0.55,
        "bucket_budget_cap": 0.7,
        "lifecycle_state": "approved",
        "allocator_version": "approved-universe-risk-budget-v1",
        "budget_reason": "default_core_allocation",
    }
    if reason == "candidate_pool_supported_by_longer_windows":
        profile.update({
            "approval_tier": "core",
            "portfolio_bucket": "core_alpha",
            "portfolio_bucket_label": "Core Alpha",
            "portfolio_weight_hint": 0.65,
            "portfolio_weight_target": 0.65,
            "max_weight": 0.4,
            "family_budget_cap": 0.55,
            "bucket_budget_cap": 0.7,
            "lifecycle_state": "approved",
            "budget_reason": "candidate_pool_supported_by_longer_windows",
        })
    elif reason == "primary_representative_watchlist_with_cross_window_support":
        profile.update({
            "approval_tier": "core",
            "portfolio_bucket": "controlled_exposure",
            "portfolio_bucket_label": "Controlled Exposure",
            "portfolio_weight_hint": 0.55,
            "portfolio_weight_target": 0.55,
            "max_weight": 0.3,
            "family_budget_cap": 0.4,
            "bucket_budget_cap": 0.45,
            "lifecycle_state": "approved",
            "budget_reason": "primary_representative_watchlist_with_cross_window_support",
        })
    elif reason == "scorecard_quality_gate":
        profile.update({
            "approval_tier": "core",
            "portfolio_bucket": "core_alpha",
            "portfolio_bucket_label": "Core Alpha",
            "portfolio_weight_hint": 0.6,
            "portfolio_weight_target": 0.6,
            "max_weight": 0.35,
            "family_budget_cap": 0.5,
            "bucket_budget_cap": 0.65,
            "lifecycle_state": "approved",
            "budget_reason": "scorecard_quality_gate",
        })
    elif reason == "representative_watchlist_bridge":
        profile.update({
            "approval_tier": "bridge",
            "portfolio_bucket": "bridge_candidate",
            "portfolio_bucket_label": "Bridge Candidate",
            "portfolio_weight_hint": 0.35,
            "portfolio_weight_target": 0.35,
            "max_weight": 0.2,
            "family_budget_cap": 0.25,
            "bucket_budget_cap": 0.25,
            "lifecycle_state": "watchlist",
            "budget_reason": "representative_watchlist_bridge",
        })
    elif reason == "bridge_recent_probe":
        profile.update({
            "approval_tier": "bridge",
            "portfolio_bucket": "recent_probe",
            "portfolio_bucket_label": "Recent Probe",
            "portfolio_weight_hint": 0.25,
            "portfolio_weight_target": 0.25,
            "max_weight": 0.12,
            "family_budget_cap": 0.2,
            "bucket_budget_cap": 0.15,
            "lifecycle_state": "shadow",
            "budget_reason": "bridge_recent_probe",
        })
    elif reason == "bridge_controlled_composite" or "Controlled Composite" in bucket_label:
        profile.update({
            "approval_tier": "bridge",
            "portfolio_bucket": "controlled_exposure",
            "portfolio_bucket_label": "Controlled Exposure",
            "portfolio_weight_hint": 0.3,
            "portfolio_weight_target": 0.3,
            "max_weight": 0.18,
            "family_budget_cap": 0.25,
            "bucket_budget_cap": 0.3,
            "lifecycle_state": "watchlist",
            "budget_reason": "bridge_controlled_composite",
        })
    profile["universe_state"] = profile["lifecycle_state"]
    return profile


def _build_lifecycle(previous_rows: list[dict[str, Any]], current_rows: list[dict[str, Any]]) -> dict[str, Any]:
    prev = {row.get("factor_name"): row for row in previous_rows if row.get("factor_name")}
    curr = {row.get("factor_name"): row for row in current_rows if row.get("factor_name")}
    entered = sorted([name for name in curr if name not in prev])
    exited = sorted([name for name in prev if name not in curr])
    stayed = sorted([name for name in curr if name in prev])
    changed = []
    transition_counts: Counter[str] = Counter()
    for name in stayed:
        prev_row = prev.get(name) or {}
        curr_row = curr.get(name) or {}
        changed_fields = []
        for key in (
            "approved_reason",
            "portfolio_bucket",
            "portfolio_weight_hint",
            "approval_tier",
            "lifecycle_state",
            "governance_action",
            "allocated_weight",
        ):
            if prev_row.get(key) != curr_row.get(key):
                changed_fields.append(key)
        prev_state = prev_row.get("lifecycle_state") or prev_row.get("universe_state")
        curr_state = curr_row.get("lifecycle_state") or curr_row.get("universe_state")
        if prev_state != curr_state and prev_state and curr_state:
            transition_counts[f"{prev_state}->{curr_state}"] += 1
        if changed_fields:
            changed.append({
                "factor_name": name,
                "changed_fields": changed_fields,
                "previous": {key: prev_row.get(key) for key in changed_fields},
                "current": {key: curr_row.get(key) for key in changed_fields},
            })
    return {
        "entered": entered,
        "exited": exited,
        "stayed": stayed,
        "changed": changed,
        "transition_counts": dict(transition_counts),
    }


def _build_governance_state(previous_state: dict[str, Any], current_rows: list[dict[str, Any]]) -> dict[str, Any]:
    row_updates: dict[str, dict[str, Any]] = {}
    state_rows: list[dict[str, Any]] = []
    action_counts: Counter[str] = Counter()
    state_counts: Counter[str] = Counter()
    transition_counts: Counter[str] = Counter()

    for row in current_rows:
        name = row.get("factor_name")
        prev = (previous_state or {}).get(name) or {}
        previous_lifecycle_state = prev.get("lifecycle_state") or row.get("lifecycle_state") or "approved"
        negative = (row.get("portfolio_contribution_class") == "negative")
        positive = (row.get("portfolio_contribution_class") == "positive")
        negative_streak = int(prev.get("negative_contribution_streak") or 0) + 1 if negative else 0
        approved_streak = int(prev.get("approved_streak") or 0) + 1
        governance_action = "keep"
        lifecycle_state = row.get("lifecycle_state") or row.get("universe_state") or "approved"
        transition_reason = "retain_current_state"

        if row.get("approval_tier") == "bridge" and negative_streak >= 2:
            governance_action = "demote_bridge_candidate"
            lifecycle_state = "rejected"
            transition_reason = "bridge_negative_contribution_streak"
        elif negative_streak >= 3:
            governance_action = "demote_candidate"
            lifecycle_state = "rejected"
            transition_reason = "negative_contribution_streak"
        elif negative:
            governance_action = "monitor_negative_contribution"
            lifecycle_state = "shadow" if lifecycle_state in {"watchlist", "shadow"} else "watchlist"
            transition_reason = "negative_contribution_requires_monitoring"
        elif positive:
            governance_action = "upweight_candidate"
            if previous_lifecycle_state in {"watchlist", "shadow"} and approved_streak >= 2:
                lifecycle_state = "approved"
                transition_reason = "positive_contribution_restores_confidence"
            else:
                transition_reason = "positive_contribution_support"
        elif row.get("approval_tier") == "bridge" and lifecycle_state == "approved":
            lifecycle_state = "watchlist"
            transition_reason = "bridge_candidates_start_in_watchlist"

        if previous_lifecycle_state != lifecycle_state:
            transition_counts[f"{previous_lifecycle_state}->{lifecycle_state}"] += 1

        update = {
            "governance_action": governance_action,
            "negative_contribution_streak": negative_streak,
            "approved_streak": approved_streak,
            "lifecycle_state": lifecycle_state,
            "universe_state": lifecycle_state,
            "previous_lifecycle_state": previous_lifecycle_state,
            "transition_reason": transition_reason,
        }
        row_updates[name] = update
        action_counts[governance_action] += 1
        state_counts[lifecycle_state] += 1
        state_rows.append({
            "factor_name": name,
            **update,
            "approval_tier": row.get("approval_tier"),
            "portfolio_bucket": row.get("portfolio_bucket"),
            "portfolio_contribution_class": row.get("portfolio_contribution_class"),
        })

    return {
        "rows": state_rows,
        "row_updates": row_updates,
        "summary": {
            "action_counts": dict(action_counts),
            "state_counts": dict(state_counts),
            "transition_counts": dict(transition_counts),
        },
    }


def _apply_portfolio_factor_budget(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    family_allocations: defaultdict[str, float] = defaultdict(float)
    bucket_allocations: defaultdict[str, float] = defaultdict(float)
    state_priority = {"approved": 0, "watchlist": 1, "shadow": 2, "rejected": 9}

    ranked_rows = sorted(
        rows,
        key=lambda r: (
            state_priority.get(r.get("lifecycle_state") or r.get("universe_state") or "approved", 9),
            0 if r.get("approval_tier") == "core" else 1,
            -(float(r.get("portfolio_weight_target") or r.get("portfolio_weight_hint") or 0.0)),
            -(int(r.get("quality_total_score") or 0)),
            r.get("factor_name") or "",
        ),
    )

    for raw_row in ranked_rows:
        row = dict(raw_row)
        state = row.get("lifecycle_state") or row.get("universe_state") or "approved"
        if state == "rejected":
            continue
        family = row.get("family") or "unknown"
        bucket = row.get("portfolio_bucket") or "core_alpha"
        target_weight = float(row.get("portfolio_weight_target") or row.get("portfolio_weight_hint") or 0.0)
        max_weight = float(row.get("max_weight") or target_weight or 0.0)
        family_cap = float(row.get("family_budget_cap") or 1.0)
        bucket_cap = float(row.get("bucket_budget_cap") or 1.0)
        remaining_family = max(0.0, family_cap - family_allocations[family])
        remaining_bucket = max(0.0, bucket_cap - bucket_allocations[bucket])
        allocated_raw = min(target_weight, max_weight, remaining_family, remaining_bucket)
        if allocated_raw <= 0:
            continue
        row["allocated_weight_raw"] = round(allocated_raw, 6)
        family_allocations[family] += allocated_raw
        bucket_allocations[bucket] += allocated_raw
        selected.append(row)

    total_weight = sum(float(row.get("allocated_weight_raw") or 0.0) for row in selected)
    if total_weight <= 0:
        total_weight = float(len(selected) or 1)
        for row in selected:
            row["allocated_weight"] = round(1.0 / total_weight, 6)
            row["portfolio_weight_hint"] = row["allocated_weight"]
    else:
        for row in selected:
            row["allocated_weight"] = round(float(row.get("allocated_weight_raw") or 0.0) / total_weight, 6)
            row["portfolio_weight_hint"] = row["allocated_weight"]
    return selected


def build_approved_candidate_universe(
    db_path: str | Path,
    *,
    recent_run_limit: int = 12,
) -> dict[str, Any]:
    db_path = Path(db_path)
    recent_artifacts = _load_recent_run_artifacts(db_path, limit=recent_run_limit)
    aggregated = _aggregate_candidate_state(db_path, recent_artifacts)
    scorecard_map = _scorecard_map(db_path, limit=5000)

    approved_rows: list[dict[str, Any]] = []
    debug_rows: list[dict[str, Any]] = []
    rejection_counter: Counter[str] = Counter()

    for name, bucket in aggregated.items():
        scorecard = scorecard_map.get(name) or bucket.get("scorecard_row") or {}
        approved, approved_reason, rejection_reasons = _approved_or_rejected(bucket)
        if not approved:
            bridge_ok, bridge_reason = _bridge_candidate(bucket)
            if bridge_ok:
                approved = True
                approved_reason = bridge_reason
                rejection_reasons = []
        row = {
            "factor_name": name,
            "expression": bucket.get("expression"),
            "family": bucket.get("family"),
            "factor_role": bucket.get("factor_role"),
            "source_run_ids": bucket.get("source_run_ids") or [],
            "source_windows": bucket.get("source_windows") or [],
            "candidate_pool_count": bucket.get("candidate_pool_count") or 0,
            "watchlist_count": bucket.get("watchlist_count") or 0,
            "graveyard_count": bucket.get("graveyard_count") or 0,
            "cross_window_support_count": bucket.get("cross_window_support_count") or 0,
            "long_window_support_count": bucket.get("long_window_support_count") or 0,
            "representative_status": "primary" if bucket.get("primary_representative_hits") else ("representative" if bucket.get("representative_hits") else "non_representative"),
            "quality_classification": scorecard.get("quality_classification"),
            "quality_total_score": scorecard.get("quality_total_score"),
            "quality_promotion_decision": scorecard.get("quality_promotion_decision"),
            "institutional_bucket_label": scorecard.get("institutional_bucket_label") or scorecard.get("effective_bucket_label"),
            "parent_delta_status": (scorecard.get("failure_dossier") or {}).get("parent_delta_status") or (bucket.get("scorecard_row") or {}).get("failure_dossier", {}).get("parent_delta_status"),
            "portfolio_contribution_class": (scorecard.get("portfolio_contribution") or {}).get("contribution_class"),
            "approved_reason": approved_reason or None,
            "rejection_reasons": rejection_reasons,
            "promotion_reasons": bucket.get("promotion_reasons") or [],
            "blocking_reasons": bucket.get("blocking_reasons") or [],
        }
        row.update(_approval_profile(row))
        debug_rows.append({**row, "approved": approved})
        if approved and row.get("expression"):
            approved_rows.append(row)
        else:
            for reason in rejection_reasons:
                rejection_counter[reason] += 1

    approved_rows.sort(
        key=lambda row: (
            {"candidate_pool_supported_by_longer_windows": 0, "primary_representative_watchlist_with_cross_window_support": 1, "scorecard_quality_gate": 2, "representative_watchlist_bridge": 3}.get(row.get("approved_reason") or "", 9),
            -(int(row.get("quality_total_score") or 0)),
            -(int(row.get("candidate_pool_count") or 0)),
            row.get("factor_name") or "",
        )
    )
    debug_rows.sort(key=lambda row: (not bool(row.get("approved")), row.get("factor_name") or ""))

    preferred_dataset_path = None
    preferred_run = recent_artifacts[0] if recent_artifacts else None
    if preferred_run and preferred_run.get("dataset_path") and Path(preferred_run["dataset_path"]).exists():
        preferred_dataset_path = str(preferred_run["dataset_path"])

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_policy_version": SELECTION_POLICY_VERSION,
        "preferred_dataset_path": preferred_dataset_path,
        "source_runs": [
            {
                "run_id": item["run"].get("run_id"),
                "created_at_utc": item["run"].get("created_at_utc"),
                "config_path": item["run"].get("config_path"),
                "output_dir": item["run"].get("output_dir"),
                "window_label": item.get("window_label"),
                "candidate_pool_count": len(item.get("candidate_pool") or []),
            }
            for item in recent_artifacts
        ],
        "summary": {
            "approved_count": len(approved_rows),
            "considered_count": len(debug_rows),
            "rejected_count": len([row for row in debug_rows if not row.get("approved")]),
            "rejection_reason_counts": dict(rejection_counter),
            "approval_tier_counts": dict(Counter(row.get("approval_tier") or "unknown" for row in approved_rows)),
            "portfolio_bucket_counts": dict(Counter(row.get("portfolio_bucket") or "unknown" for row in approved_rows)),
            "state_counts": dict(Counter((row.get("lifecycle_state") or row.get("universe_state") or "unknown") for row in approved_rows)),
        },
        "rows": approved_rows,
        "debug_rows": debug_rows,
    }


def write_approved_candidate_universe(
    db_path: str | Path,
    output_path: str | Path,
    debug_output_path: str | Path | None = None,
    lifecycle_output_path: str | Path | None = None,
    governance_output_path: str | Path | None = None,
    *,
    recent_run_limit: int = 12,
) -> dict[str, Any]:
    output_path = Path(output_path)
    previous_rows = []
    if output_path.exists():
        try:
            previous_rows = (json.loads(output_path.read_text(encoding="utf-8")) or {}).get("rows") or []
        except Exception:
            previous_rows = []

    previous_governance = {}
    if governance_output_path and Path(governance_output_path).exists():
        try:
            previous_governance_rows = (json.loads(Path(governance_output_path).read_text(encoding="utf-8")) or {}).get("rows") or []
            previous_governance = {row.get("factor_name"): row for row in previous_governance_rows if row.get("factor_name")}
        except Exception:
            previous_governance = {}

    payload = build_approved_candidate_universe(db_path=db_path, recent_run_limit=recent_run_limit)
    governance = _build_governance_state(previous_governance, payload.get("rows") or [])
    final_rows: list[dict[str, Any]] = []
    debug_rows = list(payload.get("debug_rows") or [])
    for row in payload.get("rows") or []:
        update = (governance.get("row_updates") or {}).get(row.get("factor_name")) or {}
        row.update(update)
        if row.get("governance_action") in {"demote_bridge_candidate", "demote_candidate"}:
            debug_rows.append({
                **row,
                "approved": False,
                "approved_reason": None,
                "rejection_reasons": list((row.get("rejection_reasons") or [])) + ["governance_demotion"],
            })
        else:
            final_rows.append(row)

    payload["rows"] = _apply_portfolio_factor_budget(final_rows)
    payload["debug_rows"] = debug_rows
    payload["budget_summary"] = {
        "allocator_version": "approved-universe-risk-budget-v1",
        "total_allocated_weight_raw": round(sum(float(row.get("allocated_weight_raw") or 0.0) for row in payload["rows"]), 6),
        "bucket_allocations": dict(Counter()),
        "family_allocations": dict(Counter()),
    }
    bucket_allocations: defaultdict[str, float] = defaultdict(float)
    family_allocations: defaultdict[str, float] = defaultdict(float)
    for row in payload["rows"]:
        bucket_allocations[row.get("portfolio_bucket") or "unknown"] += float(row.get("allocated_weight") or row.get("portfolio_weight_hint") or 0.0)
        family_allocations[row.get("family") or "unknown"] += float(row.get("allocated_weight") or row.get("portfolio_weight_hint") or 0.0)
    payload["budget_summary"]["bucket_allocations"] = {key: round(value, 6) for key, value in bucket_allocations.items()}
    payload["budget_summary"]["family_allocations"] = {key: round(value, 6) for key, value in family_allocations.items()}
    payload["summary"] = {
        "approved_count": len(payload["rows"]),
        "considered_count": len(payload["debug_rows"]),
        "rejected_count": len([row for row in payload["debug_rows"] if not row.get("approved")]),
        "rejection_reason_counts": dict(Counter(reason for row in payload["debug_rows"] if not row.get("approved") for reason in (row.get("rejection_reasons") or []))),
        "approval_tier_counts": dict(Counter(row.get("approval_tier") or "unknown" for row in payload["rows"])),
        "portfolio_bucket_counts": dict(Counter(row.get("portfolio_bucket") or "unknown" for row in payload["rows"])),
        "state_counts": dict(Counter((row.get("lifecycle_state") or row.get("universe_state") or "unknown") for row in payload["rows"])),
        "governance_action_counts": (governance.get("summary") or {}).get("action_counts") or {},
    }
    payload["governance"] = governance
    lifecycle = _build_lifecycle(previous_rows, payload.get("rows") or [])
    payload["lifecycle"] = lifecycle
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({k: v for k, v in payload.items() if k != "debug_rows"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if debug_output_path:
        debug_output_path = Path(debug_output_path)
        debug_output_path.parent.mkdir(parents=True, exist_ok=True)
        debug_output_path.write_text(
            json.dumps(
                {
                    "generated_at_utc": payload["generated_at_utc"],
                    "selection_policy_version": payload["selection_policy_version"],
                    "summary": payload["summary"],
                    "rows": payload["debug_rows"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    if lifecycle_output_path:
        lifecycle_output_path = Path(lifecycle_output_path)
        lifecycle_output_path.parent.mkdir(parents=True, exist_ok=True)
        lifecycle_output_path.write_text(
            json.dumps(
                {
                    "generated_at_utc": payload["generated_at_utc"],
                    "selection_policy_version": payload["selection_policy_version"],
                    "lifecycle": lifecycle,
                    "rows": payload.get("rows") or [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    if governance_output_path:
        governance_output_path = Path(governance_output_path)
        governance_output_path.parent.mkdir(parents=True, exist_ok=True)
        governance_output_path.write_text(
            json.dumps(
                {
                    "generated_at_utc": payload["generated_at_utc"],
                    "selection_policy_version": payload["selection_policy_version"],
                    "summary": governance.get("summary") or {},
                    "rows": governance.get("rows") or [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return payload


def resolve_paper_portfolio_inputs(
    db_path: str | Path,
    *,
    approved_universe_path: str | Path,
    fallback_candidate_pool_path: str | Path | None = None,
    fallback_dataset_path: str | Path | None = None,
) -> dict[str, Any]:
    approved_universe_path = Path(approved_universe_path)
    if approved_universe_path.exists():
        try:
            payload = json.loads(approved_universe_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        rows = payload.get("rows") or []
        preferred_dataset_path = payload.get("preferred_dataset_path")
        if rows and preferred_dataset_path and Path(preferred_dataset_path).exists():
            selected_rows = _apply_portfolio_factor_budget(rows)
            return {
                "source": "approved_candidate_universe",
                "dataset_path": Path(preferred_dataset_path),
                "factor_definitions": [
                    {
                        "name": row["factor_name"],
                        "expression": row["expression"],
                        "weight_hint": row.get("portfolio_weight_hint"),
                        "allocated_weight": row.get("allocated_weight"),
                        "allocated_weight_raw": row.get("allocated_weight_raw"),
                        "portfolio_weight_target": row.get("portfolio_weight_target"),
                        "portfolio_bucket": row.get("portfolio_bucket"),
                        "portfolio_bucket_label": row.get("portfolio_bucket_label"),
                        "approval_tier": row.get("approval_tier"),
                        "lifecycle_state": row.get("lifecycle_state") or row.get("universe_state"),
                        "universe_state": row.get("universe_state") or row.get("lifecycle_state"),
                        "governance_action": row.get("governance_action"),
                        "max_weight": row.get("max_weight"),
                        "family_budget_cap": row.get("family_budget_cap"),
                        "bucket_budget_cap": row.get("bucket_budget_cap"),
                        "budget_reason": row.get("budget_reason"),
                    }
                    for row in selected_rows
                    if row.get("factor_name") and row.get("expression")
                ],
                "metadata": {
                    "selection_policy_version": payload.get("selection_policy_version"),
                    "approved_count": len(rows),
                    "selected_count": len(selected_rows),
                    "portfolio_bucket_counts": payload.get("summary", {}).get("portfolio_bucket_counts") or {},
                    "state_counts": payload.get("summary", {}).get("state_counts") or {},
                    "governance_action_counts": payload.get("summary", {}).get("governance_action_counts") or {},
                    "budget_summary": payload.get("budget_summary") or {},
                },
            }

    recent_artifacts = _load_recent_run_artifacts(db_path, limit=12)
    for artifact in recent_artifacts:
        candidate_pool = artifact.get("candidate_pool") or []
        dataset_path = artifact.get("dataset_path")
        if candidate_pool and dataset_path and Path(dataset_path).exists():
            return {
                "source": "latest_non_empty_candidate_pool",
                "dataset_path": Path(dataset_path),
                "factor_definitions": [
                    {"name": row["factor_name"], "expression": row["expression"]}
                    for row in candidate_pool
                    if row.get("factor_name") and row.get("expression")
                ],
                "metadata": {
                    "run_id": artifact["run"].get("run_id"),
                    "window_label": artifact.get("window_label"),
                },
            }

    from factor_lab.paper_portfolio import resolve_latest_paper_portfolio_inputs

    latest_inputs = resolve_latest_paper_portfolio_inputs(
        db_path=db_path,
        fallback_candidate_pool_path=fallback_candidate_pool_path,
        fallback_dataset_path=fallback_dataset_path,
    )
    candidate_pool_path = Path(latest_inputs["candidate_pool_path"]) if latest_inputs.get("candidate_pool_path") else None
    dataset_path = Path(latest_inputs["dataset_path"]) if latest_inputs.get("dataset_path") else None
    factor_definitions: list[dict[str, str]] = []
    if candidate_pool_path and candidate_pool_path.exists():
        try:
            candidates = json.loads(candidate_pool_path.read_text(encoding="utf-8"))
        except Exception:
            candidates = []
        if isinstance(candidates, list):
            factor_definitions = [
                {"name": row["factor_name"], "expression": row["expression"]}
                for row in candidates
                if row.get("factor_name") and row.get("expression")
            ]
    return {
        "source": latest_inputs.get("source") or "fallback",
        "dataset_path": dataset_path,
        "factor_definitions": factor_definitions,
        "metadata": latest_inputs,
    }
