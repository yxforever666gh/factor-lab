from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timedelta
import time
from pathlib import Path
from typing import Any

from factor_lab.dedup import workflow_experiment_fingerprint
from factor_lab.factors import resolve_factor_definitions
from factor_lab.storage import ExperimentStore
from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS
from factor_lab.expression_validation import validate_expression
from factor_lab.generated_artifacts import upgrade_generated_config
from factor_lab.research_task_governance import govern_workflow_task_spec


ROOT = Path(__file__).resolve().parents[2]


def _parse_date(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d")


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _make_window_config(base_config: dict[str, Any], start_date: str, end_date: str, output_dir: str) -> dict[str, Any]:
    config = deepcopy(base_config)
    config["start_date"] = start_date
    config["end_date"] = end_date
    config["output_dir"] = output_dir
    return config


def _materialize_factor_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(config)
    factor_defs = resolve_factor_definitions(normalized, config_dir=ROOT / 'configs')
    if factor_defs:
        normalized['factors'] = factor_defs
    normalized.pop('factor_family_config', None)
    return normalized


def _light_recent_validation_config(config: dict[str, Any]) -> dict[str, Any]:
    tuned = deepcopy(config)
    tuned['universe_limit'] = min(int(tuned.get('universe_limit') or 100), int(os.getenv('RESEARCH_LIGHT_VALIDATION_UNIVERSE_LIMIT', '20')))
    rolling = dict(tuned.get('rolling_validation') or {})
    if rolling:
        rolling['window_size'] = min(int(rolling.get('window_size') or 63), int(os.getenv('RESEARCH_LIGHT_VALIDATION_ROLLING_WINDOW', '31')))
        rolling['step_size'] = min(int(rolling.get('step_size') or 21), int(os.getenv('RESEARCH_LIGHT_VALIDATION_ROLLING_STEP', '10')))
        tuned['rolling_validation'] = rolling
    tuned['validation_mode'] = 'light_recent_window'
    return tuned


def _write_generated_config(config: dict[str, Any], name: str) -> str:
    out_dir = ROOT / "artifacts" / "generated_configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.json"
    materialized = _materialize_factor_config(config)
    materialized = upgrade_generated_config(materialized, source="research_expansion")
    path.write_text(json.dumps(materialized, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path.relative_to(ROOT))


def _governance_payload_defaults(spec: dict[str, Any]) -> dict[str, Any]:
    note = str(spec.get("worker_note") or "")
    payload = dict(spec.get("payload") or {})
    payload.setdefault("hypothesis", note or "research expansion workflow should add information")
    payload.setdefault("falsification_criteria", ["net sharpe and information gain fail to improve versus recent baseline"])
    if "baseline" in note.lower():
        payload.setdefault("expected_information_gain", ["window_stability_check", "boundary_confirmed"])
        payload.setdefault("budget_bucket", "data_quality_coverage")
    else:
        payload.setdefault("expected_information_gain", ["window_stability_check", "candidate_survival_check"])
        payload.setdefault("budget_bucket", "robustness_validation")
    return payload


def _workflow_governance_used_counts(recent_tasks: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"total": 0}
    for task in recent_tasks:
        if task.get("status") not in {"pending", "running"}:
            continue
        counts["total"] += 1
        payload = task.get("payload") or {}
        bucket = payload.get("budget_bucket")
        if bucket:
            counts[str(bucket)] = counts.get(str(bucket), 0) + 1
    return counts


def _govern_expansion_workflow_spec(
    spec: dict[str, Any],
    *,
    config: dict[str, Any],
    store: ExperimentStore,
    used_counts: dict[str, int],
) -> dict[str, Any] | None:
    task_spec = deepcopy(spec)
    task_spec["payload"] = _governance_payload_defaults(task_spec)
    task_spec.pop("_governance_config", None)
    result = govern_workflow_task_spec(
        task_spec,
        config=config,
        store=store,
        used_counts=used_counts,
        source="research_expansion",
    )
    allowed = result.get("task_spec") if result.get("decision") == "allow" else None
    if not allowed:
        return None
    budget_bucket = str(((result.get("gate") or {}).get("budget_bucket")) or task_spec["payload"].get("budget_bucket") or "")
    used_counts["total"] = int(used_counts.get("total") or 0) + 1
    if budget_bucket:
        used_counts[budget_bucket] = int(used_counts.get(budget_bucket) or 0) + 1
    return allowed


def _is_generated_candidate_definition(definition: dict[str, Any]) -> bool:
    name = str(definition.get("name") or "")
    return (
        name.startswith("gen__")
        or bool(definition.get("generator_operator"))
        or bool(definition.get("left_factor_name"))
        or bool(definition.get("right_factor_name"))
    )


def _compile_generated_candidate_expression(operator: str, left_expr: str, right_expr: str) -> str | None:
    if operator == 'combine_add':
        return f'({left_expr}) + ({right_expr})'
    if operator == 'combine_sub':
        return f'({left_expr}) - ({right_expr})'
    if operator == 'combine_ratio':
        return f'({left_expr}) / ({right_expr})'
    if operator == 'combine_mul':
        return f'({left_expr}) * ({right_expr})'
    if operator == 'combine_avg':
        return f'(({left_expr}) + ({right_expr})) / 2'
    if operator == 'combine_primary_bias':
        return f'((2 * ({left_expr})) + ({right_expr})) / 3'
    return None


def _materialize_generated_candidate_definition(base_recent: dict[str, Any], definition: dict[str, Any]) -> dict[str, Any] | None:
    resolved = resolve_factor_definitions(deepcopy(base_recent), config_dir=ROOT / 'configs') or []
    factor_map = {row.get('name'): row for row in resolved if isinstance(row, dict) and row.get('name')}
    left_name = definition.get('left_factor_name')
    right_name = definition.get('right_factor_name')
    operator = definition.get('generator_operator')
    left_expr = ((factor_map.get(left_name) or {}).get('expression')) if left_name else None
    right_expr = ((factor_map.get(right_name) or {}).get('expression')) if right_name else None
    if not operator or not left_expr or not right_expr:
        return None
    compiled = _compile_generated_candidate_expression(str(operator), str(left_expr), str(right_expr))
    if not compiled:
        return None
    materialized = deepcopy(definition)
    materialized['expression'] = compiled
    return materialized


def _light_generated_candidate_config(base_recent: dict[str, Any], definition: dict[str, Any], *, start_date: str, end_date: str, output_dir: str) -> dict[str, Any]:
    materialized = _materialize_generated_candidate_definition(base_recent, definition)
    if materialized is None:
        raise ValueError(f"generated candidate definition cannot be materialized: {definition.get('name')}")
    cfg = deepcopy(base_recent)
    cfg["factors"] = [materialized]
    cfg["start_date"] = start_date
    cfg["end_date"] = end_date
    cfg["output_dir"] = output_dir
    cfg["universe_limit"] = min(int(cfg.get("universe_limit") or 100), int(os.getenv("RESEARCH_GENERATED_CANDIDATE_UNIVERSE_LIMIT", "20")))
    rolling = dict(cfg.get("rolling_validation") or {})
    if rolling:
        rolling["window_size"] = min(int(rolling.get("window_size") or 63), int(os.getenv("RESEARCH_GENERATED_CANDIDATE_ROLLING_WINDOW", "21")))
        rolling["step_size"] = min(int(rolling.get("step_size") or 21), int(os.getenv("RESEARCH_GENERATED_CANDIDATE_ROLLING_STEP", "7")))
        cfg["rolling_validation"] = rolling
    cfg["generated_candidate_validation_mode"] = "light"
    return cfg


def _candidate_validation_specs(store: ExperimentStore, base_recent: dict[str, Any], end_date: str) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    end_dt = _parse_date(end_date)
    promising = store.top_promising_candidates(limit=4)
    for idx, candidate in enumerate(promising):
        definition = candidate.get("definition") or {}
        if not definition.get("name"):
            continue
        expression = definition.get("expression") or ""
        if expression:
            validation = validate_expression(expression, available_fields=TUSHARE_FEATURE_COLUMNS)
            if not validation.ok:
                # Skip deterministic invalid expressions (prevents circuit breaker stalls).
                continue
        is_generated_candidate = _is_generated_candidate_definition(definition)
        if is_generated_candidate and os.getenv("RESEARCH_ENABLE_GENERATED_CANDIDATE_REVALIDATION", "0").strip().lower() not in {"1", "true", "yes", "on"}:
            continue
        windows = [(45, 12)] if is_generated_candidate else [(45, 12), (90, 14)]
        for days, priority in windows:
            name = f"candidate_{definition['name']}_recent_{days}d"
            output_dir = f"artifacts/generated_candidate_{definition['name']}_recent_{days}d"
            if is_generated_candidate:
                try:
                    cfg = _light_generated_candidate_config(
                        base_recent,
                        definition,
                        start_date=_fmt_date(end_dt - timedelta(days=days)),
                        end_date=end_date,
                        output_dir=output_dir,
                    )
                except ValueError:
                    continue
            else:
                cfg = deepcopy(base_recent)
                cfg["factors"] = [definition]
                cfg["start_date"] = _fmt_date(end_dt - timedelta(days=days))
                cfg["end_date"] = end_date
                cfg["output_dir"] = output_dir
            config_path = _write_generated_config(cfg, name)
            fingerprint = workflow_experiment_fingerprint(cfg)
            payload = {"config_path": config_path, "output_dir": output_dir}
            if is_generated_candidate:
                payload["source"] = "candidate_generation_validation"
                payload["validation_stage"] = f"recent_{days}d_light"
            specs.append(
                {
                    "task_type": "workflow",
                    "priority": priority + idx,
                    "payload": payload,
                    "fingerprint": fingerprint,
                    "worker_note": (
                        f"validation｜candidate_generation {definition['name']} recent_{days}d｜light"
                        if is_generated_candidate
                        else f"validation｜candidate_validation {definition['name']} recent_{days}d"
                    ),
                    "_governance_config": cfg,
                }
            )
    return specs


def expansion_candidates(store: ExperimentStore, *, allow_repeat: bool = False) -> list[dict[str, Any]]:
    recent_tasks = store.list_research_tasks(limit=300)
    existing_fingerprints = {
        t.get("fingerprint") for t in recent_tasks if t.get("status") in {"pending", "running", "finished"}
    }

    # When breaking stagnation, we may intentionally repeat expansions.
    # In that mode we still avoid duplicating *pending/running* work, but we allow new unique runs.
    pending_or_running_fingerprints = {
        t.get("fingerprint") for t in recent_tasks if t.get("status") in {"pending", "running"}
    }
    candidates: list[dict[str, Any]] = []

    base_recent = json.loads((ROOT / "configs" / "tushare_workflow.json").read_text(encoding="utf-8"))
    end_date = base_recent["end_date"]
    recent_start = _parse_date(base_recent["start_date"])
    end_dt = _parse_date(end_date)

    windows = [
        {
            "name": "rolling_30d_back",
            "start_date": _fmt_date(recent_start - timedelta(days=30)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_rolling_30d_back",
            "priority": 18,
            "worker_note": "baseline｜历史扩窗 30 天",
        },
        {
            "name": "rolling_60d_back",
            "start_date": _fmt_date(recent_start - timedelta(days=60)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_rolling_60d_back",
            "priority": 19,
            "worker_note": "baseline｜历史扩窗 60 天",
        },
        {
            "name": "rolling_120d_back",
            "start_date": _fmt_date(recent_start - timedelta(days=120)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_rolling_120d_back",
            "priority": 20,
            "worker_note": "baseline｜历史扩窗 120 天",
        },
        {
            "name": "rolling_recent_45d",
            "start_date": _fmt_date(end_dt - timedelta(days=45)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_recent_45d",
            "priority": 22,
            "worker_note": "validation｜近期 45 天窗口验证｜light",
            "light_recent_validation": True,
        },
        {
            "name": "rolling_recent_90d",
            "start_date": _fmt_date(end_dt - timedelta(days=90)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_recent_90d",
            "priority": 24,
            "worker_note": "validation｜近期 90 天窗口验证｜light",
            "light_recent_validation": True,
        },
        {
            "name": "rolling_recent_120d",
            "start_date": _fmt_date(end_dt - timedelta(days=120)),
            "end_date": end_date,
            "output_dir": "artifacts/generated_recent_120d",
            "priority": 25,
            "worker_note": "validation｜近期 120 天窗口验证｜light",
            "light_recent_validation": True,
        },
        {
            "name": "expanding_from_2025_10_01",
            "start_date": "2025-10-01",
            "end_date": end_date,
            "output_dir": "artifacts/generated_expanding_2025_10_01",
            "priority": 16,
            "worker_note": "baseline｜expanding 窗口 2025-10-01 起",
        },
    ]

    used_counts = _workflow_governance_used_counts(recent_tasks)

    for spec in _candidate_validation_specs(store, base_recent, end_date) + windows:
        if "start_date" in spec:
            config = _make_window_config(base_recent, spec["start_date"], spec["end_date"], spec["output_dir"])
            if spec.get('light_recent_validation'):
                config = _light_recent_validation_config(config)
            fingerprint = workflow_experiment_fingerprint(config)
            if (not allow_repeat and fingerprint in existing_fingerprints) or (fingerprint in pending_or_running_fingerprints):
                continue
            config_path = _write_generated_config(config, spec["name"])
            payload = {"config_path": config_path, "output_dir": spec["output_dir"]}
            if spec.get('light_recent_validation'):
                payload['source'] = 'recent_window_validation_light'
                payload['validation_stage'] = 'recent_window_light'
            candidate = {
                "task_type": "workflow",
                "priority": spec["priority"],
                "payload": payload,
                "fingerprint": fingerprint,
                "worker_note": spec["worker_note"],
            }
            governed = _govern_expansion_workflow_spec(candidate, config=config, store=store, used_counts=used_counts)
            if governed:
                candidates.append(governed)
            continue
        if (not allow_repeat and spec["fingerprint"] in existing_fingerprints) or (spec["fingerprint"] in pending_or_running_fingerprints):
            continue
        config = spec.get("_governance_config")
        if not isinstance(config, dict):
            continue
        governed = _govern_expansion_workflow_spec(spec, config=config, store=store, used_counts=used_counts)
        if governed:
            candidates.append(governed)

    return candidates


def maybe_expand_research_space(store: ExperimentStore, max_new_tasks: int = 3, *, allow_repeat: bool = False) -> list[str]:
    recent_tasks = store.list_research_tasks(limit=50)
    pending_or_running = [t for t in recent_tasks if t["status"] in {"pending", "running"}]
    if pending_or_running:
        return []

    task_specs = expansion_candidates(store, allow_repeat=allow_repeat)[:max_new_tasks]

    # If we're breaking stagnation and have nothing new, force a unique repeat baseline run.
    if not task_specs and allow_repeat:
        base_recent = json.loads((ROOT / "configs" / "tushare_workflow.json").read_text(encoding="utf-8"))
        nonce = int(time.time())
        output_dir = f"artifacts/forced_expand_{nonce}"
        cfg = deepcopy(base_recent)
        cfg["output_dir"] = output_dir
        config_path = _write_generated_config(cfg, f"forced_expand_{nonce}")
        fingerprint = workflow_experiment_fingerprint(cfg)
        candidate = {
            "task_type": "workflow",
            "priority": 26,
            "payload": {"config_path": config_path, "output_dir": output_dir},
            "fingerprint": fingerprint,
            "worker_note": "exploration｜forced expansion to break stagnation",
        }
        governed = _govern_expansion_workflow_spec(
            candidate,
            config=cfg,
            store=store,
            used_counts=_workflow_governance_used_counts(recent_tasks),
        )
        task_specs = [governed] if governed else []
    new_task_ids = []
    for spec in task_specs:
        task_id = store.enqueue_research_task(
            task_type=spec["task_type"],
            payload=spec["payload"],
            priority=spec["priority"],
            fingerprint=spec["fingerprint"],
            worker_note=spec["worker_note"],
        )
        new_task_ids.append(task_id)
    return new_task_ids
