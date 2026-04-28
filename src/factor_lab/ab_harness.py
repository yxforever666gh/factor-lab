from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from factor_lab.expression_validation import validate_expression
from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS
from factor_lab.frontier_policy import build_frontier_focus
from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.storage import ExperimentStore
from factor_lab.generated_artifacts import upgrade_generated_batch, upgrade_generated_config


ROOT = Path(__file__).resolve().parents[2]
INPUT_SNAPSHOT_FILENAME = "input_snapshot.json"


@dataclass(frozen=True)
class HarnessWindowSpec:
    label: str
    days: int | None = None
    expanding: bool = False


DEFAULT_HARNESS_WINDOWS = [
    HarnessWindowSpec(label="recent_30d", days=30),
    HarnessWindowSpec(label="recent_45d", days=45),
    HarnessWindowSpec(label="recent_60d", days=60),
    HarnessWindowSpec(label="recent_90d", days=90),
    HarnessWindowSpec(label="recent_120d", days=120),
    HarnessWindowSpec(label="expanding", expanding=True),
]

MODE_LABELS = {
    "legacy": "旧路",
    "frontier": "新路",
}


def _frontier_preferred_quality_ok(row: dict[str, Any]) -> bool:
    avg_score = float(row.get("avg_final_score") or 0.0)
    latest_score = float(row.get("latest_final_score") or 0.0)
    pass_rate = float(row.get("pass_rate") or 0.0)
    robustness_score = float(row.get("robustness_score") or 0.0)
    risk_score = float(row.get("risk_score") or 100.0)
    split_fail_count = int(row.get("split_fail_count") or 0)

    # Preferred frontier names already come pre-deduped, so keep the bar focused on
    # recent strength and robustness instead of re-rejecting the cluster leader.
    return (
        latest_score >= 7.0
        and robustness_score >= 0.75
        and risk_score < 85.0
        and split_fail_count <= 1
        and (avg_score >= 5.0 or pass_rate >= 0.25 or latest_score >= 9.0)
    )



def _frontier_secondary_quality_ok(row: dict[str, Any]) -> bool:
    avg_score = float(row.get("avg_final_score") or 0.0)
    latest_score = float(row.get("latest_final_score") or 0.0)
    pass_rate = float(row.get("pass_rate") or 0.0)
    robustness_score = float(row.get("robustness_score") or 0.0)
    risk_score = float(row.get("risk_score") or 100.0)
    split_fail_count = int(row.get("split_fail_count") or 0)
    duplicate_peer_count = int(row.get("duplicate_peer_count") or 0)

    return (
        avg_score >= 5.0
        and (latest_score >= 7.0 or pass_rate >= 0.45)
        and robustness_score >= 0.75
        and risk_score < 85.0
        and split_fail_count <= 1
        and duplicate_peer_count == 0
        and (pass_rate >= 0.2 or latest_score >= 9.0)
    )


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d")


def _format_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _default_expanding_start_date(end_date: str) -> str:
    end_dt = _parse_date(end_date)
    anchor = (end_dt - timedelta(days=180)).replace(day=1)
    return _format_date(anchor)


def _slug(text: str) -> str:
    keep = []
    for char in text:
        if char.isalnum() or char in {"_", "-"}:
            keep.append(char)
        else:
            keep.append("_")
    return "".join(keep).strip("_")


def _write_generated_config(config: dict[str, Any], name: str, folder: str) -> str:
    out_dir = ROOT / "artifacts" / folder
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    payload = upgrade_generated_config(config, source="ab_harness")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path.relative_to(ROOT))


def _load_candidate_rows(db_path: str | Path, limit: int = 200) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    store = ExperimentStore(db_path)
    rows = store.list_factor_candidates(limit=limit)
    return rows, {row["name"]: row for row in rows}



def _load_candidate_rows_from_snapshot(snapshot_payload: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows = list(snapshot_payload.get("candidate_rows") or [])
    return rows, {row["name"]: row for row in rows if row.get("name")}



def _dedupe_names(names: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if not name or name in seen:
            continue
        ordered.append(name)
        seen.add(name)
    return ordered



def _load_research_frontier_context() -> dict[str, Any]:
    candidate_pool_path = ROOT / "artifacts" / "research_candidate_pool.json"
    research_memory_path = ROOT / "artifacts" / "research_memory.json"

    candidate_pool = json.loads(candidate_pool_path.read_text(encoding="utf-8")) if candidate_pool_path.exists() else {}
    research_memory = json.loads(research_memory_path.read_text(encoding="utf-8")) if research_memory_path.exists() else {}

    primary_representatives: list[str] = []
    secondary_representatives: list[tuple[int, str]] = []
    for row in candidate_pool.get("representative_selection") or []:
        name = row.get("candidate")
        if not name:
            continue
        if row.get("is_primary_representative"):
            primary_representatives.append(name)
            continue
        rank = row.get("representative_rank")
        if rank is not None:
            secondary_representatives.append((int(rank), name))

    task_focus: list[str] = []
    for task in candidate_pool.get("tasks") or []:
        focus_candidates = [row.get("candidate_name") for row in (task.get("focus_candidates") or []) if row.get("candidate_name")]
        if focus_candidates:
            task_focus = focus_candidates
            break

    return {
        "primary_representatives": _dedupe_names(primary_representatives),
        "secondary_representatives": _dedupe_names([name for _, name in sorted(secondary_representatives)]),
        "task_focus": _dedupe_names(task_focus),
        "stable_candidates": _dedupe_names(research_memory.get("stable_candidates") or []),
        "suppressed_candidates": sorted(set(research_memory.get("suppressed_candidates") or [])),
    }



def _frontier_research_priority_ok(row: dict[str, Any]) -> bool:
    avg_score = float(row.get("avg_final_score") or 0.0)
    latest_score = float(row.get("latest_final_score") or 0.0)
    pass_rate = float(row.get("pass_rate") or 0.0)
    robustness_score = float(row.get("robustness_score") or 0.0)
    risk_score = float(row.get("risk_score") or 100.0)
    split_fail_count = int(row.get("split_fail_count") or 0)

    return (
        robustness_score >= 0.7
        and risk_score < 95.0
        and split_fail_count <= 1
        and (avg_score >= 2.0 or pass_rate >= 0.2 or latest_score >= 7.0)
    )



def _capture_legacy_run_rows(db_path: str | Path, latest_run: dict[str, Any] | None, limit: int = 10) -> list[dict[str, Any]]:
    if not latest_run:
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT factor_name, rank_ic_mean, score, pass_gate, split_fail_count, fail_reason
            FROM factor_results
            WHERE run_id = ? AND variant = 'raw_scored'
            ORDER BY pass_gate DESC, score DESC, rank_ic_mean DESC, factor_name ASC
            LIMIT ?
            """,
            (latest_run.get("run_id"), limit),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()



def _capture_input_snapshot(db_path: str | Path, candidate_limit: int = 300, scorecard_limit: int = 50) -> dict[str, Any]:
    candidate_rows, _ = _load_candidate_rows(db_path, limit=candidate_limit)
    scorecard = build_promotion_scorecard(db_path=db_path, limit=scorecard_limit)
    latest_run = scorecard.get("latest_run") or {}
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "candidate_limit": candidate_limit,
        "scorecard_limit": scorecard_limit,
        "candidate_rows": candidate_rows,
        "scorecard": scorecard,
        "research_frontier": _load_research_frontier_context(),
        "legacy_run_rows": _capture_legacy_run_rows(db_path, latest_run, limit=max(10, scorecard_limit)),
    }



def _load_or_create_input_snapshot(
    *,
    db_path: str | Path,
    output_root: Path,
    refresh: bool = False,
    candidate_limit: int = 300,
    scorecard_limit: int = 50,
) -> tuple[dict[str, Any], Path]:
    snapshot_path = output_root / INPUT_SNAPSHOT_FILENAME
    if snapshot_path.exists() and not refresh:
        return json.loads(snapshot_path.read_text(encoding="utf-8")), snapshot_path

    snapshot_payload = _capture_input_snapshot(
        db_path=db_path,
        candidate_limit=candidate_limit,
        scorecard_limit=scorecard_limit,
    )
    snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_payload, snapshot_path



def select_legacy_candidates(snapshot_payload: dict[str, Any], top_n: int = 3) -> list[dict[str, Any]]:
    _, candidate_by_name = _load_candidate_rows_from_snapshot(snapshot_payload)
    legacy_run_rows = snapshot_payload.get("legacy_run_rows") or []
    selected = []
    for row in legacy_run_rows:
        name = row.get("factor_name")
        candidate_row = candidate_by_name.get(name) or {}
        if not name or not candidate_row.get("expression"):
            continue
        selected.append(
            {
                "factor_name": name,
                "family": candidate_row.get("family"),
                "expression": candidate_row.get("expression"),
                "source": "legacy_latest_official_run",
                "latest_final_score": row.get("score"),
                "avg_final_score": candidate_row.get("avg_final_score"),
                "status": candidate_row.get("status"),
                "legacy_run_rank_ic_mean": row.get("rank_ic_mean"),
                "legacy_run_pass_gate": row.get("pass_gate"),
            }
        )
        if len(selected) >= top_n:
            break

    if selected:
        return selected

    # Last-resort fallback: keep prior behavior if the snapshot lacks usable run rows.
    for candidate_row in candidate_by_name.values():
        if not candidate_row.get("expression"):
            continue
        selected.append(
            {
                "factor_name": candidate_row["name"],
                "family": candidate_row.get("family"),
                "expression": candidate_row.get("expression"),
                "source": "legacy_latest_score_fallback",
                "latest_final_score": candidate_row.get("latest_final_score"),
                "avg_final_score": candidate_row.get("avg_final_score"),
                "status": candidate_row.get("status"),
            }
        )
        if len(selected) >= top_n:
            break
    return selected


def select_frontier_candidates(snapshot_payload: dict[str, Any], top_n: int = 3) -> list[dict[str, Any]]:
    _, candidate_by_name = _load_candidate_rows_from_snapshot(snapshot_payload)
    scorecard = snapshot_payload.get("scorecard") or {}
    focus = build_frontier_focus(scorecard)
    research_frontier = snapshot_payload.get("research_frontier") or {}
    scorecard_rows = scorecard.get("rows") or []
    scorecard_by_name = {row.get("factor_name"): row for row in scorecard_rows if row.get("factor_name")}

    selected_names: list[str] = []
    research_priority_names = _dedupe_names(
        research_frontier.get("primary_representatives")
        + research_frontier.get("task_focus")
        + research_frontier.get("stable_candidates")
    )
    research_support_names = _dedupe_names(research_frontier.get("secondary_representatives") or [])
    scorecard_priority_names = _dedupe_names(focus.get("preferred_candidates") or [])
    scorecard_secondary_names = _dedupe_names(focus.get("secondary_candidates") or [])
    suppressed = set(focus.get("suppressed_candidates") or []) | set(research_frontier.get("suppressed_candidates") or set())

    for name in research_priority_names:
        # Research-priority names are already the system's chosen frontier anchors.
        # Keep them as-is so the harness measures the frontier policy itself rather
        # than a second scorecard filter layered on top.
        if name in selected_names:
            continue
        selected_names.append(name)
        if len(selected_names) >= top_n:
            break

    if len(selected_names) < top_n:
        for name in research_support_names + scorecard_priority_names:
            if name in selected_names or (name in suppressed and name not in research_priority_names):
                continue
            score_row = scorecard_by_name.get(name) or {}
            if not _frontier_preferred_quality_ok(score_row):
                continue
            selected_names.append(name)
            if len(selected_names) >= top_n:
                break

    if not selected_names:
        for name in scorecard_secondary_names:
            if name in suppressed:
                continue
            score_row = scorecard_by_name.get(name) or {}
            if not _frontier_secondary_quality_ok(score_row):
                continue
            selected_names.append(name)
            if len(selected_names) >= top_n:
                break

    if not selected_names:
        fallback_rows = sorted(
            [
                row
                for row in scorecard_rows
                if row.get("factor_name")
                and row.get("factor_name") not in suppressed
                and row.get("decision_key") in {"core_candidate", "validate_now", "dedupe_first"}
                and _frontier_preferred_quality_ok(row)
            ],
            key=lambda row: (
                -float(row.get("promotion_score") or 0.0),
                -float(row.get("latest_final_score") or 0.0),
                -float(row.get("avg_final_score") or 0.0),
                row.get("factor_name") or "",
            ),
        )
        for row in fallback_rows:
            selected_names.append(row["factor_name"])
            if len(selected_names) >= top_n:
                break

    selected = []
    research_names = set(research_priority_names) | set(research_support_names)
    for name in selected_names:
        row = candidate_by_name.get(name)
        score_row = scorecard_by_name.get(name) or {}
        if not row or not row.get("expression"):
            continue
        selected.append(
            {
                "factor_name": row["name"],
                "family": row.get("family"),
                "expression": row.get("expression"),
                "source": "frontier_focus_research_anchor" if name in research_names else "frontier_focus_strict",
                "latest_final_score": row.get("latest_final_score"),
                "avg_final_score": row.get("avg_final_score"),
                "pass_rate": row.get("pass_rate"),
                "promotion_score": score_row.get("promotion_score"),
                "status": row.get("status"),
            }
        )
    return selected


def _build_mode_jobs(
    *,
    mode: str,
    selected: list[dict[str, Any]],
    base_config: dict[str, Any],
    window_specs: list[HarnessWindowSpec],
    expanding_start_date: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    jobs: list[dict[str, Any]] = []
    manifest: list[dict[str, Any]] = []
    end_date = base_config["end_date"]
    end_dt = _parse_date(end_date)

    for row in selected:
        expression = row.get("expression") or ""
        validation = validate_expression(expression, available_fields=TUSHARE_FEATURE_COLUMNS)
        if not validation.ok:
            continue

        factor_name = row["factor_name"]
        factor_manifest = {
            **row,
            "window_jobs": [],
        }
        for spec in window_specs:
            if spec.expanding:
                start_date = expanding_start_date
                window_key = f"expanding_{start_date.replace('-', '_')}"
            else:
                start_date = _format_date(end_dt - timedelta(days=int(spec.days or 0)))
                window_key = spec.label

            job_name = f"{mode}__{_slug(factor_name)}__{window_key}"
            job_output_dir = f"artifacts/ab_harness/{mode}/{job_name}"
            config = deepcopy(base_config)
            config["factors"] = [{"name": factor_name, "expression": expression}]
            config["start_date"] = start_date
            config["end_date"] = end_date
            config["output_dir"] = job_output_dir
            config_path = _write_generated_config(config, job_name, "generated_ab_configs")

            jobs.append({"name": job_name, "config_path": config_path})
            factor_manifest["window_jobs"].append(
                {
                    "job_name": job_name,
                    "window_label": window_key,
                    "start_date": start_date,
                    "end_date": end_date,
                    "config_path": config_path,
                    "output_dir": job_output_dir,
                }
            )
        manifest.append(factor_manifest)
    return jobs, manifest


def build_ab_harness_plan(
    *,
    db_path: str | Path,
    base_config_path: str | Path,
    output_root: str | Path,
    top_n: int = 3,
    window_specs: list[HarnessWindowSpec] | None = None,
    refresh_input_snapshot: bool = False,
) -> dict[str, Any]:
    db_path = Path(db_path)
    base_config = json.loads(Path(base_config_path).read_text(encoding="utf-8"))
    output_root = Path(output_root)
    if not output_root.is_absolute():
        output_root = ROOT / output_root
    output_root.mkdir(parents=True, exist_ok=True)
    window_specs = window_specs or DEFAULT_HARNESS_WINDOWS
    expanding_start_date = _default_expanding_start_date(base_config["end_date"])
    input_snapshot, input_snapshot_path = _load_or_create_input_snapshot(
        db_path=db_path,
        output_root=output_root,
        refresh=refresh_input_snapshot,
    )

    legacy = select_legacy_candidates(input_snapshot, top_n=top_n)
    frontier = select_frontier_candidates(input_snapshot, top_n=top_n)

    mode_payloads: dict[str, Any] = {}
    for mode, selected in (("legacy", legacy), ("frontier", frontier)):
        jobs, manifest = _build_mode_jobs(
            mode=mode,
            selected=selected,
            base_config=base_config,
            window_specs=window_specs,
            expanding_start_date=expanding_start_date,
        )
        batch_path = output_root / f"{mode}_batch.json"
        batch_payload = upgrade_generated_batch({"jobs": jobs}, source="ab_harness")
        batch_path.write_text(json.dumps(batch_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        mode_payloads[mode] = {
            "label": MODE_LABELS[mode],
            "batch_config_path": str(batch_path.relative_to(ROOT)),
            "batch_output_dir": f"artifacts/ab_harness/{mode}",
            "selected_candidates": manifest,
            "job_count": len(jobs),
        }

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "base_config_path": str(Path(base_config_path)),
        "output_root": str(output_root.relative_to(ROOT)),
        "input_snapshot_path": str(input_snapshot_path.relative_to(ROOT)),
        "input_snapshot_generated_at_utc": input_snapshot.get("generated_at_utc"),
        "top_n": top_n,
        "window_labels": [spec.label for spec in window_specs],
        "expanding_start_date": expanding_start_date,
        "modes": mode_payloads,
    }
    plan_path = output_root / "plan.json"
    plan_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _relationship_metrics(db_path: str | Path, selected_names: list[str]) -> dict[str, Any]:
    store = ExperimentStore(db_path)
    selected = set(selected_names)
    duplicate_pairs = 0
    high_corr_pairs = 0
    refinement_edges = 0
    for row in store.list_candidate_relationships(limit=5000):
        left = row.get("left_name")
        right = row.get("right_name")
        if left not in selected or right not in selected:
            continue
        relationship_type = row.get("relationship_type")
        if relationship_type == "duplicate_of":
            duplicate_pairs += 1
        elif relationship_type == "high_corr":
            high_corr_pairs += 1
        elif relationship_type == "refinement_of":
            refinement_edges += 1
    return {
        "duplicate_pairs": duplicate_pairs,
        "high_corr_pairs": high_corr_pairs,
        "refinement_edges": refinement_edges,
    }


def _job_metrics(job_dir: Path) -> dict[str, Any]:
    candidate_pool = _read_json(job_dir / "candidate_pool.json", [])
    graveyard = _read_json(job_dir / "factor_graveyard.json", [])
    raw_results = _read_json(job_dir / "results.json", [])
    neutralized_results = _read_json(job_dir / "neutralized_results.json", [])
    split_results = _read_json(job_dir / "split_results.json", [])
    raw_row = raw_results[0] if raw_results else {}
    neutral_row = neutralized_results[0] if neutralized_results else {}
    split_fail_count = len([row for row in split_results if not row.get("pass_gate")])
    return {
        "candidate_count": len(candidate_pool),
        "graveyard_count": len(graveyard),
        "is_candidate": bool(candidate_pool),
        "is_graveyard": bool(graveyard),
        "raw_rank_ic_mean": raw_row.get("rank_ic_mean"),
        "neutralized_rank_ic_mean": neutral_row.get("rank_ic_mean"),
        "neutralized_pass_gate": bool(neutral_row.get("pass_gate")) if neutral_row else False,
        "split_fail_count": split_fail_count,
    }


def summarize_ab_harness(plan_path: str | Path, db_path: str | Path, output_path: str | Path | None = None) -> dict[str, Any]:
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    db_path = Path(db_path)
    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "plan_path": str(plan_path),
        "modes": {},
        "comparison": {},
    }

    for mode, payload in (plan.get("modes") or {}).items():
        selected = payload.get("selected_candidates") or []
        selected_names = [row.get("factor_name") for row in selected if row.get("factor_name")]
        factor_summaries = []
        short_survivors = 0
        medium_survivors = 0
        expanding_survivors = 0
        neutralized_pass_total = 0
        raw_ic_values = []
        neutral_ic_values = []
        split_fail_total = 0
        total_job_count = 0
        total_graveyard_count = 0

        for row in selected:
            job_rows = []
            medium_hits = 0
            short_hits = 0
            expanding_hits = 0
            neutral_hits = 0
            for job in row.get("window_jobs") or []:
                job_dir = ROOT / job["output_dir"]
                metrics = _job_metrics(job_dir)
                job_rows.append({**job, **metrics})
                label = job.get("window_label") or ""
                if metrics["is_candidate"]:
                    if label.startswith("recent_30") or label.startswith("recent_45"):
                        short_hits += 1
                    elif label.startswith("recent_60") or label.startswith("recent_90") or label.startswith("recent_120"):
                        medium_hits += 1
                    elif label.startswith("expanding"):
                        expanding_hits += 1
                if metrics["neutralized_pass_gate"]:
                    neutral_hits += 1
                if metrics.get("raw_rank_ic_mean") is not None:
                    raw_ic_values.append(float(metrics["raw_rank_ic_mean"]))
                if metrics.get("neutralized_rank_ic_mean") is not None:
                    neutral_ic_values.append(float(metrics["neutralized_rank_ic_mean"]))
                split_fail_total += int(metrics.get("split_fail_count") or 0)
                total_job_count += 1
                total_graveyard_count += 1 if metrics.get("is_graveyard") else 0

            short_survivors += 1 if short_hits > 0 else 0
            medium_survivors += 1 if medium_hits > 0 else 0
            expanding_survivors += 1 if expanding_hits > 0 else 0
            neutralized_pass_total += 1 if neutral_hits > 0 else 0
            factor_summaries.append(
                {
                    "factor_name": row.get("factor_name"),
                    "source": row.get("source"),
                    "family": row.get("family"),
                    "short_window_hits": short_hits,
                    "medium_window_hits": medium_hits,
                    "expanding_hits": expanding_hits,
                    "neutralized_window_hits": neutral_hits,
                    "graveyard_rate": round(
                        len([job for job in job_rows if job.get("is_graveyard")]) / max(len(job_rows), 1),
                        6,
                    ),
                    "window_decay_slope": short_hits - medium_hits - expanding_hits,
                    "job_rows": job_rows,
                }
            )

        relationship_metrics = _relationship_metrics(db_path, selected_names)
        selected_count = len(selected_names)
        mode_summary = {
            "label": payload.get("label") or mode,
            "selected_candidates": selected_names,
            "selected_count": selected_count,
            "short_window_survivor_count": short_survivors,
            "medium_window_survivor_count": medium_survivors,
            "expanding_survivor_count": expanding_survivors,
            "neutralized_survivor_count": neutralized_pass_total,
            "avg_raw_rank_ic_mean": round(sum(raw_ic_values) / len(raw_ic_values), 6) if raw_ic_values else None,
            "avg_neutralized_rank_ic_mean": round(sum(neutral_ic_values) / len(neutral_ic_values), 6) if neutral_ic_values else None,
            "graveyard_rate": round(total_graveyard_count / max(total_job_count, 1), 6),
            "window_decay_score": short_survivors - medium_survivors - expanding_survivors,
            "split_fail_total": split_fail_total,
            "relationship_metrics": relationship_metrics,
            "factor_summaries": factor_summaries,
        }
        result["modes"][mode] = mode_summary

    legacy = result["modes"].get("legacy") or {}
    frontier = result["modes"].get("frontier") or {}
    result["comparison"] = {
        "winner_by_medium_survivors": (
            "frontier" if (frontier.get("medium_window_survivor_count") or 0) > (legacy.get("medium_window_survivor_count") or 0)
            else "legacy" if (legacy.get("medium_window_survivor_count") or 0) > (frontier.get("medium_window_survivor_count") or 0)
            else "tie"
        ),
        "medium_survivor_delta": (frontier.get("medium_window_survivor_count") or 0) - (legacy.get("medium_window_survivor_count") or 0),
        "neutralized_survivor_delta": (frontier.get("neutralized_survivor_count") or 0) - (legacy.get("neutralized_survivor_count") or 0),
        "graveyard_rate_delta": round((frontier.get("graveyard_rate") or 0.0) - (legacy.get("graveyard_rate") or 0.0), 6),
        "window_decay_delta": (frontier.get("window_decay_score") or 0) - (legacy.get("window_decay_score") or 0),
        "duplicate_pair_delta": (
            (frontier.get("relationship_metrics") or {}).get("duplicate_pairs") or 0
        ) - (
            (legacy.get("relationship_metrics") or {}).get("duplicate_pairs") or 0
        ),
        "high_corr_pair_delta": (
            (frontier.get("relationship_metrics") or {}).get("high_corr_pairs") or 0
        ) - (
            (legacy.get("relationship_metrics") or {}).get("high_corr_pairs") or 0
        ),
    }

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def write_ab_harness_markdown(summary_payload: dict[str, Any], output_path: str | Path) -> None:
    lines = ["# A/B Harness Summary", ""]
    comparison = summary_payload.get("comparison") or {}
    lines.append(f"- Winner by medium-window survivors: {comparison.get('winner_by_medium_survivors')}")
    lines.append(f"- Medium survivor delta (frontier - legacy): {comparison.get('medium_survivor_delta')}")
    lines.append(f"- Neutralized survivor delta (frontier - legacy): {comparison.get('neutralized_survivor_delta')}")
    lines.append(f"- Graveyard rate delta (frontier - legacy): {comparison.get('graveyard_rate_delta')}")
    lines.append(f"- Window decay delta (frontier - legacy): {comparison.get('window_decay_delta')}")
    lines.append(f"- Duplicate pair delta (frontier - legacy): {comparison.get('duplicate_pair_delta')}")
    lines.append("")

    for mode in ("legacy", "frontier"):
        payload = (summary_payload.get("modes") or {}).get(mode) or {}
        lines.extend(
            [
                f"## {payload.get('label') or mode}",
                f"- Selected candidates: {', '.join(payload.get('selected_candidates') or []) or 'none'}",
                f"- Short-window survivors: {payload.get('short_window_survivor_count')}",
                f"- Medium-window survivors: {payload.get('medium_window_survivor_count')}",
                f"- Expanding survivors: {payload.get('expanding_survivor_count')}",
                f"- Neutralized survivors: {payload.get('neutralized_survivor_count')}",
                f"- Avg raw RankIC mean: {payload.get('avg_raw_rank_ic_mean')}",
                f"- Avg neutralized RankIC mean: {payload.get('avg_neutralized_rank_ic_mean')}",
                f"- Graveyard rate: {payload.get('graveyard_rate')}",
                f"- Window decay score: {payload.get('window_decay_score')}",
                f"- Relationship metrics: {json.dumps(payload.get('relationship_metrics') or {}, ensure_ascii=False)}",
                "",
            ]
        )
        for factor in payload.get("factor_summaries") or []:
            lines.append(
                f"- {factor.get('factor_name')} | short={factor.get('short_window_hits')} | medium={factor.get('medium_window_hits')} | expanding={factor.get('expanding_hits')} | neutralized={factor.get('neutralized_window_hits')}"
            )
        lines.append("")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
