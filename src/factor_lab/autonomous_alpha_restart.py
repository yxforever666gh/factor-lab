from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACTS_DIR = ROOT / "artifacts"
DEFAULT_OUTPUT_DIR = DEFAULT_ARTIFACTS_DIR / "autonomous_alpha_restart"
BENCHMARK_ROUTE = "value_quality_no_distress"
BENCHMARK_BUCKET_SPREAD = 0.0062253011

FAILED_OR_CLOSED_ROUTES = {
    "cashflow_value_trap": "closed_monitor_only",
    "value_trap_filter_quality_confirmation": "closed_cashflow_conditioning_non_incremental",
    "repaired_debt_to_assets_reverse": "failed_bucket_aware_spread_negative",
    "pit_non_cashflow_value_trap_expansion": "no_mechanism_passed_preflight",
}


@dataclass(frozen=True)
class EvidenceRun:
    path: str
    route_id: str | None
    factor_name: str | None
    expression: str | None
    rank_ic_mean: float | None
    rank_ic_ir: float | None
    top_bottom_spread_mean: float | None
    sharpe_net: float | None
    bucket_spread_mean: float | None
    bucket_pass_gate: bool | None
    observations: int | None
    classification: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def infer_route_id(path: Path, factor_name: str | None, expression: str | None) -> str | None:
    text = "/".join(path.parts[-6:])
    candidates = [
        "value_quality_no_distress",
        "value_momentum_confirmation",
        "industry_relative_value",
        "value_trap_filter_quality_confirmation",
        "cashflow_only_denominator_repaired",
        "repaired_debt_to_assets_reverse",
    ]
    blob = " ".join(x for x in [text, factor_name or "", expression or ""] if x)
    for candidate in candidates:
        if candidate in blob:
            return candidate
    if "operating_cashflow_to_profit" in blob:
        return "cashflow_only_denominator_repaired"
    return None


def _classify_run(bucket_spread: float | None, bucket_pass: bool | None, rank_ic: float | None, sharpe: float | None, route_id: str | None) -> str:
    if route_id in FAILED_OR_CLOSED_ROUTES:
        return "known_failed_or_closed"
    if bucket_pass is True and bucket_spread is not None and bucket_spread > 0:
        return "bucket_aware_success"
    if rank_ic is not None and rank_ic > 0.02 and (sharpe is None or sharpe < 1.0):
        return "signal_without_tradeable_gate"
    if bucket_spread is not None and bucket_spread <= 0:
        return "bucket_aware_failure"
    if rank_ic is not None and rank_ic <= 0:
        return "weak_or_negative_signal"
    return "unknown_or_engineering_evidence"


def scan_evidence_runs(artifacts_dir: Path = DEFAULT_ARTIFACTS_DIR) -> list[EvidenceRun]:
    runs: list[EvidenceRun] = []
    for results_path in artifacts_dir.rglob("results.json"):
        data = _read_json(results_path)
        if not isinstance(data, list) or not data:
            continue
        first = data[0] if isinstance(data[0], dict) else {}
        bucket_path = results_path.parent / "bucket_aware_portfolio_results.json"
        bucket_data = _read_json(bucket_path)
        bucket_first = bucket_data[0] if isinstance(bucket_data, list) and bucket_data and isinstance(bucket_data[0], dict) else {}
        route_id = infer_route_id(results_path.parent, first.get("factor_name"), first.get("expression"))
        rank_ic = _as_float(first.get("rank_ic_mean"))
        sharpe = _as_float(first.get("sharpe_net"))
        bucket_spread = _as_float(bucket_first.get("spread_mean"))
        bucket_pass = bucket_first.get("pass_gate") if isinstance(bucket_first.get("pass_gate"), bool) else None
        try:
            display_path = str(results_path.parent.relative_to(ROOT))
        except ValueError:
            display_path = str(results_path.parent)
        runs.append(
            EvidenceRun(
                path=display_path,
                route_id=route_id,
                factor_name=first.get("factor_name"),
                expression=first.get("expression"),
                rank_ic_mean=rank_ic,
                rank_ic_ir=_as_float(first.get("rank_ic_ir")),
                top_bottom_spread_mean=_as_float(first.get("top_bottom_spread_mean")),
                sharpe_net=sharpe,
                bucket_spread_mean=bucket_spread,
                bucket_pass_gate=bucket_pass,
                observations=_as_int(first.get("observations") or bucket_first.get("observations")),
                classification=_classify_run(bucket_spread, bucket_pass, rank_ic, sharpe, route_id),
            )
        )
    return runs


def build_restart_boundary() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "generated_at_utc": _now(),
        "decision": "restart_with_closed_failed_routes_and_read_only_selection",
        "closed_routes": FAILED_OR_CLOSED_ROUTES,
        "workflow_execution_allowed_in_this_plan": False,
        "blocked_before_selected_probe_plan": [
            "cashflow_value_trap",
            "value_trap_filter_quality_confirmation",
            "repaired_debt_to_assets_reverse",
            "old_generated_recent_rolling_paths",
        ],
        "allowed_actions": [
            "read_artifacts",
            "score_mechanism_candidates",
            "write_one_probe_plan_or_no_probe_plan",
            "run_tests_and_runtime_audits",
        ],
    }


def build_historical_alpha_map(runs: list[EvidenceRun]) -> dict[str, Any]:
    route_rows: dict[str, dict[str, Any]] = {}
    for run in runs:
        key = run.route_id or "unknown"
        row = route_rows.setdefault(
            key,
            {"route_id": key, "runs": 0, "bucket_successes": 0, "max_bucket_spread": None, "best_path": None, "classifications": {}},
        )
        row["runs"] += 1
        row["classifications"][run.classification] = row["classifications"].get(run.classification, 0) + 1
        if run.bucket_pass_gate:
            row["bucket_successes"] += 1
        if run.bucket_spread_mean is not None and (row["max_bucket_spread"] is None or run.bucket_spread_mean > row["max_bucket_spread"]):
            row["max_bucket_spread"] = run.bucket_spread_mean
            row["best_path"] = run.path
    rows = sorted(route_rows.values(), key=lambda r: (r.get("max_bucket_spread") is not None, r.get("max_bucket_spread") or -999), reverse=True)
    return {
        "schema_version": 1,
        "generated_at_utc": _now(),
        "total_runs_scanned": len(runs),
        "benchmark": {"route_id": BENCHMARK_ROUTE, "bucket_spread_mean": BENCHMARK_BUCKET_SPREAD},
        "routes": rows,
        "interpretation": {
            "primary_positive_route": BENCHMARK_ROUTE,
            "primary_positive_route_caveat": "other promoted value routes are highly correlated and were collapsed into one value sleeve",
            "failed_or_closed_routes": FAILED_OR_CLOSED_ROUTES,
        },
    }


def build_failed_route_registry() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "generated_at_utc": _now(),
        "routes": [
            {"route_id": route, "status": status, "may_reopen_only_with": "new_written_plan_and_positive_read_only_evidence"}
            for route, status in FAILED_OR_CLOSED_ROUTES.items()
        ],
    }


def build_mechanism_candidates(alpha_map: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        {
            "candidate_id": "value_quality_low_crowding_confirmation",
            "mechanism_family": "non_cashflow_confirmation",
            "hypothesis": "The existing value_quality_no_distress sleeve may improve when upper-middle value/quality names with lower turnover shock are preferred, avoiding crowded or liquidity-stressed tails.",
            "required_fields": ["industry_relative_book_yield", "roe", "turnover_shock_5_20"],
            "evidence_refs": [
                "value_quality_no_distress remains the primary benchmark after sleeve collapse",
                "turnover_shock_5_20 was weak as a standalone alpha but may be useful as a risk/confirmation filter",
                "cashflow conditioning failed, so use non-cashflow tradability/crowding evidence instead",
            ],
            "expected_incremental_value": "May improve bucket-aware Q3-Q0 by filtering crowded or unstable value names without reopening cashflow value-trap.",
            "failure_standard": "Fail if read-only or controlled probe bucket-aware spread does not exceed 0.006225 or if turnover filter reduces observations/stability materially.",
            "next_action": "write_controlled_probe_plan",
            "blocked_by_failed_route": False,
        },
        {
            "candidate_id": "middle_bucket_value_regime_policy",
            "mechanism_family": "portfolio_construction",
            "hypothesis": "The alpha is concentrated in upper-middle value buckets rather than extreme top value; future probes should test regime/bucket policy rather than raw expression variants.",
            "required_fields": ["industry_relative_book_yield", "roe"],
            "evidence_refs": ["value route diagnostics showed middle-hump and Q3 outperformed Q4", "value_quality_no_distress Q3-Q0 spread 0.006225"],
            "expected_incremental_value": "Could reduce tail noise, but much of this has already been validated in bucket-aware routes.",
            "failure_standard": "No new workflow unless it proposes a genuinely new construction not already covered by bucket-aware follow-ups.",
            "next_action": "document_as_policy_baseline",
            "blocked_by_failed_route": False,
        },
        {
            "candidate_id": "anti_overgrowth_value_confirmation",
            "mechanism_family": "reverse_growth_confirmation",
            "hypothesis": "Overheated growth signals may be negatively priced in the 2020-2023 universe; value names with non-extreme or reversed growth may outperform.",
            "required_fields": ["industry_relative_book_yield", "netprofit_yoy", "tr_yoy"],
            "evidence_refs": ["netprofit_yoy reversed had weak positive diagnostics", "tr_yoy was weak and blocked earnings-stability mechanism"],
            "expected_incremental_value": "Possible anti-overgrowth filter, but evidence is incomplete because revenue growth did not confirm.",
            "failure_standard": "Block unless read-only scoring shows both profit and revenue growth conditions add to value_quality_no_distress.",
            "next_action": "read_only_only",
            "blocked_by_failed_route": False,
        },
        {
            "candidate_id": "balance_sheet_liquidity_distress_full",
            "mechanism_family": "balance_sheet_distress",
            "hypothesis": "Cheap firms with weak current/quick liquidity and high leverage may be value traps.",
            "required_fields": ["debt_to_assets", "current_ratio", "quick_ratio"],
            "evidence_refs": ["debt-only degraded variant already failed", "current_ratio and quick_ratio are not surfaced"],
            "expected_incremental_value": "Only possible after new fields are surfaced; do not run degraded debt-only again.",
            "failure_standard": "Blocked until current_ratio and quick_ratio coverage >=60% and read-only direction evidence is positive.",
            "next_action": "data_surface_plan_only",
            "blocked_by_failed_route": True,
        },
        {
            "candidate_id": "profitability_margin_quality_full",
            "mechanism_family": "profitability_quality",
            "hypothesis": "Persistent profitability and margins may separate cheap quality from cheap traps.",
            "required_fields": ["verified_pit_roe", "grossprofit_margin", "netprofit_margin"],
            "evidence_refs": ["current roe is ambiguous legacy vs confirmed PIT", "grossprofit_margin and netprofit_margin are not surfaced"],
            "expected_incremental_value": "Potentially better than cashflow, but not runnable until PIT fields are surfaced and verified.",
            "failure_standard": "Blocked until PIT-safe fields exist and have >=60% complete-case coverage.",
            "next_action": "data_surface_plan_only",
            "blocked_by_failed_route": False,
        },
        {
            "candidate_id": "cashflow_value_trap_reopen",
            "mechanism_family": "closed_cashflow",
            "hypothesis": "Closed control candidate included only to prove the restart gate blocks cashflow resurrection.",
            "required_fields": ["operating_cashflow_to_profit"],
            "evidence_refs": ["cashflow-only probe did not beat value_quality_no_distress", "cashflow conditioning was non-incremental"],
            "expected_incremental_value": "None under current evidence.",
            "failure_standard": "Always blocked unless future reopen plan exists with positive read-only evidence.",
            "next_action": "blocked",
            "blocked_by_failed_route": True,
        },
    ]
    return {"schema_version": 1, "generated_at_utc": _now(), "candidates": candidates}


def score_mechanism_candidates(candidates_doc: dict[str, Any]) -> dict[str, Any]:
    weights = {
        "historical_evidence": 25,
        "benchmark_increment_potential": 20,
        "field_availability": 15,
        "non_duplicate": 15,
        "robustness": 10,
        "falsifiability": 10,
        "implementation_simplicity": 5,
    }
    manual_scores = {
        "value_quality_low_crowding_confirmation": dict(historical_evidence=20, benchmark_increment_potential=16, field_availability=15, non_duplicate=13, robustness=7, falsifiability=9, implementation_simplicity=5),
        "middle_bucket_value_regime_policy": dict(historical_evidence=24, benchmark_increment_potential=8, field_availability=15, non_duplicate=4, robustness=9, falsifiability=8, implementation_simplicity=5),
        "anti_overgrowth_value_confirmation": dict(historical_evidence=9, benchmark_increment_potential=9, field_availability=12, non_duplicate=13, robustness=4, falsifiability=9, implementation_simplicity=4),
        "balance_sheet_liquidity_distress_full": dict(historical_evidence=5, benchmark_increment_potential=12, field_availability=0, non_duplicate=12, robustness=3, falsifiability=8, implementation_simplicity=2),
        "profitability_margin_quality_full": dict(historical_evidence=6, benchmark_increment_potential=14, field_availability=0, non_duplicate=14, robustness=4, falsifiability=8, implementation_simplicity=2),
        "cashflow_value_trap_reopen": dict(historical_evidence=0, benchmark_increment_potential=0, field_availability=15, non_duplicate=0, robustness=0, falsifiability=10, implementation_simplicity=5),
    }
    rows = []
    for cand in candidates_doc.get("candidates", []):
        cid = cand["candidate_id"]
        component = manual_scores.get(cid, {})
        score = sum(float(component.get(k, 0.0)) for k in weights)
        hard_blocks = []
        if cand.get("blocked_by_failed_route"):
            hard_blocks.append("blocked_by_failed_or_closed_route")
        if cand.get("next_action") == "data_surface_plan_only":
            hard_blocks.append("missing_surfaced_fields")
        if cid == "middle_bucket_value_regime_policy":
            hard_blocks.append("mostly_already_validated_policy_not_new_probe")
        if cid == "cashflow_value_trap_reopen":
            hard_blocks.append("cashflow_closure_policy")
        decision = "eligible_for_one_probe_plan" if score >= 70 and not hard_blocks else "not_selected"
        rows.append({**cand, "score": score, "component_scores": component, "hard_blocks": hard_blocks, "decision": decision})
    selected = [r for r in rows if r["decision"] == "eligible_for_one_probe_plan"]
    selected = sorted(selected, key=lambda r: r["score"], reverse=True)[:1]
    return {
        "schema_version": 1,
        "generated_at_utc": _now(),
        "threshold": 70,
        "benchmark": {"route_id": BENCHMARK_ROUTE, "bucket_spread_mean": BENCHMARK_BUCKET_SPREAD},
        "scores": rows,
        "selected_probe_candidate": selected[0] if selected else None,
        "selection_decision": "write_one_controlled_probe_plan" if selected else "no_probe_write_data_or_evidence_plan",
    }


def _md_restart_boundary(doc: dict[str, Any]) -> str:
    lines = ["# Autonomous Alpha Restart Boundary", "", f"Decision: `{doc['decision']}`", "", "Closed/blocked routes:"]
    lines += [f"- `{k}`: {v}" for k, v in doc["closed_routes"].items()]
    lines += ["", f"Workflow execution allowed in this plan: `{doc['workflow_execution_allowed_in_this_plan']}`"]
    return "\n".join(lines) + "\n"


def _md_alpha_map(doc: dict[str, Any]) -> str:
    lines = ["# Historical Alpha Map", "", f"Runs scanned: {doc['total_runs_scanned']}", "", f"Benchmark: `{BENCHMARK_ROUTE}` bucket spread `{BENCHMARK_BUCKET_SPREAD}`", "", "Top routes by bucket spread:"]
    for row in doc.get("routes", [])[:20]:
        lines.append(f"- `{row['route_id']}`: runs={row['runs']}, bucket_successes={row['bucket_successes']}, max_bucket_spread={row['max_bucket_spread']}, best_path={row['best_path']}")
    return "\n".join(lines) + "\n"


def _md_candidates(doc: dict[str, Any]) -> str:
    lines = ["# Mechanism Candidates", ""]
    for cand in doc.get("candidates", []):
        lines.append(f"## {cand['candidate_id']}")
        lines.append(f"Hypothesis: {cand['hypothesis']}")
        lines.append(f"Required fields: {', '.join(cand['required_fields'])}")
        lines.append(f"Next action: `{cand['next_action']}`")
        lines.append("")
    return "\n".join(lines)


def _md_scores(doc: dict[str, Any]) -> str:
    lines = ["# Mechanism Scores", "", f"Decision: `{doc['selection_decision']}`", ""]
    for row in sorted(doc.get("scores", []), key=lambda r: r["score"], reverse=True):
        lines.append(f"- `{row['candidate_id']}`: score={row['score']:.1f}, decision={row['decision']}, hard_blocks={row['hard_blocks']}")
    selected = doc.get("selected_probe_candidate")
    lines.append("")
    lines.append(f"Selected: `{selected['candidate_id'] if selected else 'none'}`")
    return "\n".join(lines) + "\n"


def write_outputs(output_dir: Path = DEFAULT_OUTPUT_DIR, artifacts_dir: Path = DEFAULT_ARTIFACTS_DIR) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    boundary = build_restart_boundary()
    runs = scan_evidence_runs(artifacts_dir)
    alpha_map = build_historical_alpha_map(runs)
    failed_registry = build_failed_route_registry()
    candidates = build_mechanism_candidates(alpha_map)
    scores = score_mechanism_candidates(candidates)

    outputs = {
        "restart_boundary.json": boundary,
        "historical_alpha_map.json": alpha_map,
        "failed_route_registry.json": failed_registry,
        "mechanism_candidates.json": candidates,
        "mechanism_scores.json": scores,
        "selected_probe_candidate.json": scores.get("selected_probe_candidate") or {"selected_probe_candidate": None},
    }
    for name, doc in outputs.items():
        (output_dir / name).write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "evidence_index.jsonl").write_text("".join(json.dumps(r.to_dict(), ensure_ascii=False) + "\n" for r in runs), encoding="utf-8")
    (output_dir / "restart_boundary.md").write_text(_md_restart_boundary(boundary), encoding="utf-8")
    (output_dir / "historical_alpha_map.md").write_text(_md_alpha_map(alpha_map), encoding="utf-8")
    (output_dir / "mechanism_candidates.md").write_text(_md_candidates(candidates), encoding="utf-8")
    (output_dir / "mechanism_scores.md").write_text(_md_scores(scores), encoding="utf-8")
    selected = scores.get("selected_probe_candidate")
    (output_dir / "selected_probe_candidate.md").write_text(_md_scores(scores), encoding="utf-8")
    return {
        "output_dir": str(output_dir),
        "runs_scanned": len(runs),
        "selection_decision": scores.get("selection_decision"),
        "selected_candidate_id": selected.get("candidate_id") if selected else None,
        "selected_score": selected.get("score") if selected else None,
    }
