from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.dedup import workflow_experiment_fingerprint
from factor_lab.workflow_admission_adapter import enforce_workflow_admission

DEFAULT_PARENT_CONFIG_PATH = Path("artifacts/pledge_controlled_probe_plan/value_quality_high_pledge_record_count_confirmation.json")
DEFAULT_VALIDATION_PATH = Path("artifacts/pledge_controlled_validation/pledge_controlled_validation.json")
DEFAULT_OUTPUT_DIR = Path("artifacts/pledge_followup_probe")

EXPECTED_ROUTE_ID = "value_quality_high_pledge_record_count_confirmation"
EXPECTED_MECHANISM_ID = "pledge_control_pressure"
FOLLOWUP_TYPE = "cost_sensitivity_20bps"
FOLLOWUP_COST_BPS = 20.0
BENCHMARK_SPREAD = 0.0062253011


def _read_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_pledge_followup_config(
    parent_config: dict[str, Any],
    *,
    output_root: str = "artifacts/pledge_followup_probe/runs",
) -> dict[str, Any]:
    """Build exactly one semantically distinct pledge follow-up config.

    The variant changes trading-cost assumptions only.  Output paths and transient
    metadata are deliberately not the only changes: the semantic workflow
    fingerprint changes through ``portfolio_cost_bps_per_turnover`` and
    ``followup_type``.
    """
    cfg = deepcopy(parent_config)
    route_id = str(cfg.get("route_id") or EXPECTED_ROUTE_ID)
    cfg["route_id"] = route_id
    cfg["mechanism_id"] = cfg.get("mechanism_id") or EXPECTED_MECHANISM_ID
    cfg["source"] = "controlled_pledge_probe_followup"
    cfg["followup_type"] = FOLLOWUP_TYPE
    cfg["portfolio_cost_bps_per_turnover"] = FOLLOWUP_COST_BPS
    cfg["expected_new_evidence"] = "pledge_cost_robustness_20bps_after_readonly_bucket_pass"
    cfg["followup_of"] = {
        "parent_route_id": route_id,
        "parent_source": parent_config.get("source"),
        "parent_expected_new_evidence": parent_config.get("expected_new_evidence"),
        "parent_portfolio_cost_bps_per_turnover": parent_config.get("portfolio_cost_bps_per_turnover"),
    }
    cfg["output_dir"] = f"{output_root}/{route_id}__{FOLLOWUP_TYPE}"
    return cfg


def _payload_from_config(config_path: str | Path, cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "config_path": str(config_path),
        "output_dir": str(cfg.get("output_dir") or DEFAULT_OUTPUT_DIR / "runs" / EXPECTED_ROUTE_ID),
        "mechanism_id": cfg.get("mechanism_id"),
        "route_id": cfg.get("route_id"),
        "followup_type": cfg.get("followup_type"),
        "required_data_fields": cfg.get("required_data_fields") or [],
        "factors": cfg.get("factors") or [],
        "portfolio_construction": cfg.get("portfolio_construction") or {},
        "source": cfg.get("source"),
        "feature_overlay_csv": cfg.get("feature_overlay_csv"),
        "feature_overlay_columns": cfg.get("feature_overlay_columns") or [],
        "expected_new_evidence": cfg.get("expected_new_evidence"),
        "benchmark": cfg.get("benchmark") or {},
    }


def _validation_evidence(validation: dict[str, Any]) -> dict[str, Any]:
    bucket = (
        validation.get("reported_bucket_aware_result")
        or validation.get("recomputed_bucket_aware_result")
        or validation.get("bucket_aware")
        or validation.get("bucket_aware_recomputed")
        or {}
    )
    coverage = validation.get("coverage") or {}
    decision = validation.get("decision")
    # Be tolerant of historical artifact shape while keeping the reported fields stable.
    spread = bucket.get("spread_mean")
    if spread is None:
        spread = bucket.get("q3_q0_spread") or bucket.get("bucket_spread")
    observations = bucket.get("observations") or bucket.get("observation_count")
    factor_non_null_rate = coverage.get("factor_non_null_rate")
    if factor_non_null_rate is None:
        factor_non_null_rate = coverage.get("factor_nonnull_rate")
    factor_non_null_tickers = coverage.get("factor_non_null_tickers")
    if factor_non_null_tickers is None:
        factor_non_null_tickers = coverage.get("factor_nonnull_tickers")
    return {
        "decision": decision,
        "bucket_spread": spread,
        "bucket_observations": observations,
        "factor_non_null_rate": factor_non_null_rate,
        "factor_non_null_tickers": factor_non_null_tickers,
        "benchmark_spread": BENCHMARK_SPREAD,
        "spread_above_benchmark": (float(spread) > BENCHMARK_SPREAD) if spread is not None else None,
    }


def build_pledge_followup_probe_result(
    *,
    parent_config_path: str | Path = DEFAULT_PARENT_CONFIG_PATH,
    validation_path: str | Path = DEFAULT_VALIDATION_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    parent_path = Path(parent_config_path)
    out = Path(output_dir)
    config_path = out / f"{EXPECTED_ROUTE_ID}__{FOLLOWUP_TYPE}.json"
    parent = _read_json(parent_path)
    validation = _read_json(validation_path) if Path(validation_path).exists() else {}
    followup = build_pledge_followup_config(parent)
    # Config generation is part of the dry-run artifact step.  Admission loads
    # config_path from disk, so write the candidate config before evaluating
    # admission while still avoiding any DB/queue write.
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(followup, ensure_ascii=False, indent=2), encoding="utf-8")
    parent_fp = workflow_experiment_fingerprint(parent)
    followup_fp = workflow_experiment_fingerprint(followup)
    payload = _payload_from_config(config_path, followup)
    task = {
        "task_type": "workflow",
        "payload": payload,
        "worker_note": f"controlled_pledge_probe_followup｜{EXPECTED_ROUTE_ID}｜{FOLLOWUP_TYPE}",
    }
    admission = enforce_workflow_admission(task)
    checks = {
        "route_exact": followup.get("route_id") == EXPECTED_ROUTE_ID,
        "mechanism_exact": followup.get("mechanism_id") == EXPECTED_MECHANISM_ID,
        "source_exact": followup.get("source") == "controlled_pledge_probe_followup",
        "followup_type_exact": followup.get("followup_type") == FOLLOWUP_TYPE,
        "cost_20bps": float(followup.get("portfolio_cost_bps_per_turnover") or 0.0) == FOLLOWUP_COST_BPS,
        "fingerprint_differs_from_parent": parent_fp != followup_fp,
        "admission_allow": admission.get("decision") == "allow",
    }
    ok = all(checks.values())
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": "dry_run_allow_exactly_one" if ok else "block",
        "ok": ok,
        "write_db": False,
        "no_queue_write": True,
        "would_enqueue_count": 1 if ok else 0,
        "enqueued_count": 0,
        "parent_config_path": str(parent_path),
        "followup_config_path": str(config_path),
        "checks": checks,
        "parent_fingerprint": parent_fp,
        "followup_fingerprint": followup_fp,
        "fingerprint_differs_from_parent": parent_fp != followup_fp,
        "validation_evidence": _validation_evidence(validation),
        "admission": {"decision": admission.get("decision"), "reasons": admission.get("reasons") or []},
        "task": task,
        "config": followup,
    }


def write_pledge_followup_probe(
    *,
    parent_config_path: str | Path = DEFAULT_PARENT_CONFIG_PATH,
    validation_path: str | Path = DEFAULT_VALIDATION_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    out = Path(output_dir)
    result = build_pledge_followup_probe_result(
        parent_config_path=parent_config_path,
        validation_path=validation_path,
        output_dir=out,
    )
    out.mkdir(parents=True, exist_ok=True)
    config_path = Path(result["followup_config_path"])
    config_path.write_text(json.dumps(result["config"], ensure_ascii=False, indent=2), encoding="utf-8")
    artifact = {key: value for key, value in result.items() if key != "config"}
    (out / "pledge_followup_probe_dry_run.json").write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "pledge_followup_probe_dry_run.md").write_text(render_pledge_followup_probe_markdown(artifact), encoding="utf-8")
    return artifact


def render_pledge_followup_probe_markdown(result: dict[str, Any]) -> str:
    evidence = result.get("validation_evidence") or {}
    admission = result.get("admission") or {}
    lines = [
        "# Pledge follow-up controlled probe dry-run admission",
        "",
        f"Decision: `{result.get('decision')}`",
        f"OK: `{result.get('ok')}`",
        f"Would enqueue count: `{result.get('would_enqueue_count')}`",
        f"Enqueued count: `{result.get('enqueued_count')}`",
        f"No queue write: `{result.get('no_queue_write')}`",
        "",
        "## Follow-up",
        f"- Route: `{EXPECTED_ROUTE_ID}`",
        f"- Follow-up type: `{FOLLOWUP_TYPE}`",
        f"- Cost bps per turnover: `{FOLLOWUP_COST_BPS}`",
        f"- Config: `{result.get('followup_config_path')}`",
        "",
        "## Parent validation evidence",
        f"- Bucket spread: `{evidence.get('bucket_spread')}`",
        f"- Benchmark: `{evidence.get('benchmark_spread')}`",
        f"- Spread above benchmark: `{evidence.get('spread_above_benchmark')}`",
        f"- Bucket observations: `{evidence.get('bucket_observations')}`",
        f"- Factor non-null tickers: `{evidence.get('factor_non_null_tickers')}`",
        "",
        "## Fingerprints",
        f"- Parent: `{result.get('parent_fingerprint')}`",
        f"- Follow-up: `{result.get('followup_fingerprint')}`",
        f"- Differs: `{result.get('fingerprint_differs_from_parent')}`",
        "",
        "## Admission",
        f"- Decision: `{admission.get('decision')}`",
        f"- Reasons: `{admission.get('reasons')}`",
        "",
        "## Checks",
    ]
    for name, passed in (result.get("checks") or {}).items():
        lines.append(f"- {name}: `{passed}`")
    lines.append("")
    return "\n".join(lines)
