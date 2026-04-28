from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.exploration_pools import classify_exploration_pool
from factor_lab.factors import resolve_factor_definitions

ROOT = Path(__file__).resolve().parents[2]
BASE_CONFIG_PATH = ROOT / "configs" / "tushare_workflow.json"
PRIMITIVE_LIBRARY_PATH = ROOT / "configs" / "research_factor_primitives.json"
GENERATED_CONFIG_DIR = ROOT / "artifacts" / "generated_candidate_configs"


def _read_json(path: str | Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _base_config() -> dict[str, Any]:
    config = _read_json(BASE_CONFIG_PATH, {})
    factor_defs = resolve_factor_definitions(config, config_dir=BASE_CONFIG_PATH.resolve().parent)
    primitive_defs = list((_read_json(PRIMITIVE_LIBRARY_PATH, {}) or {}).get("factors") or [])
    merged_defs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in list(factor_defs or []) + primitive_defs:
        name = row.get("name") if isinstance(row, dict) else None
        if not name or name in seen:
            continue
        seen.add(name)
        merged_defs.append(row)
    if merged_defs:
        config["factors"] = merged_defs
        config.pop("factor_family_config", None)
    return config


def _compile_expression(operator: str, left_expr: str, right_expr: str) -> str:
    if operator == "combine_add":
        return f"({left_expr}) + ({right_expr})"
    if operator == "combine_sub":
        return f"({left_expr}) - ({right_expr})"
    if operator == "combine_ratio":
        return f"({left_expr}) / ({right_expr})"
    if operator == "combine_mul":
        return f"({left_expr}) * ({right_expr})"
    if operator == "combine_avg":
        return f"(({left_expr}) + ({right_expr})) / 2"
    if operator == "combine_primary_bias":
        return f"((2 * ({left_expr})) + ({right_expr})) / 3"
    if operator == "residualize_against_peer":
        return f"residualize(({left_expr}), ({right_expr}))"
    if operator == "orthogonalize_against_peer":
        return f"orthogonalize(({left_expr}), ({right_expr}))"
    raise ValueError(f"unsupported operator: {operator}")


def compile_candidate_generation_plan(plan_path: str | Path) -> list[dict[str, Any]]:
    plan = _read_json(plan_path, {})
    proposals = list(plan.get("proposals") or [])
    base = _base_config()
    factor_map = {row["name"]: row for row in base.get("factors", []) if row.get("name")}
    tasks: list[dict[str, Any]] = []

    for proposal in proposals:
        if not ((proposal.get("cheap_screen") or {}).get("pass", True)):
            continue
        base_factors = [name for name in (proposal.get("base_factors") or []) if name in factor_map]
        if len(base_factors) < 2:
            continue
        left = deepcopy(factor_map[base_factors[0]])
        right = deepcopy(factor_map[base_factors[1]])
        expression = _compile_expression(proposal.get("operator"), left["expression"], right["expression"])
        candidate_name = proposal.get("candidate_id")
        generated = {
            "name": candidate_name,
            "expression": expression,
            "family": proposal.get("target_family") or "generated",
            "role": "family_probe",
            "allow_in_portfolio": True,
            "generator_operator": proposal.get("operator"),
            "left_factor_name": left["name"],
            "right_factor_name": right["name"],
        }

        cfg = deepcopy(base)
        cfg["output_dir"] = f"artifacts/generated_candidate_runs/{candidate_name}"
        cfg["factors"] = [left, right, generated]
        cfg["candidate_generation_context"] = proposal

        config_path = GENERATED_CONFIG_DIR / f"{candidate_name}.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        fingerprint = f"workflow::{config_fingerprint(cfg)}::{cfg['output_dir']}"
        triage = dict(proposal.get("triage") or {})
        triage_score = float(triage.get("score") or 0.0)
        triage_label = triage.get("label") or ("high" if triage_score >= 0.67 else "medium" if triage_score >= 0.48 else "low")
        triage.setdefault("score", round(triage_score, 6))
        triage.setdefault("label", triage_label)
        priority_hint = 55
        if triage_score >= 0.67:
            priority_hint = 41
        elif triage_score >= 0.48:
            priority_hint = 49
        exploration_pool = classify_exploration_pool(proposal.get("source"), proposal)
        tasks.append({
            "task_type": "workflow",
            "category": "exploration",
            "priority_hint": priority_hint,
            "reason": (proposal.get("rationale") or "candidate generation proposal") + f" triage={triage_label}:{triage_score:.3f}.",
            "goal": f"validate_generated_candidate:{candidate_name}",
            "hypothesis": proposal.get("rationale") or candidate_name,
            "branch_id": candidate_name,
            "expected_knowledge_gain": proposal.get("expected_information_gain") or ["candidate_survival_check"],
            "payload": {
                "config_path": str(config_path.relative_to(ROOT)),
                "output_dir": cfg["output_dir"],
                "goal": f"validate_generated_candidate:{candidate_name}",
                "hypothesis": proposal.get("rationale") or candidate_name,
                "branch_id": candidate_name,
                "source": "candidate_generation",
                "expected_information_gain": proposal.get("expected_information_gain") or ["candidate_survival_check"],
                "candidate_generation_context": proposal,
                "triage": triage,
                "exploration_pool": exploration_pool,
                "mechanism_novelty_class": proposal.get("mechanism_novelty_class") or ("new_mechanism" if exploration_pool.endswith("exploration") else "old_space"),
                "decision_source": proposal.get("decision_source"),
                "novelty_judgment_source": proposal.get("novelty_judgment_source"),
                "mechanism_rationale": proposal.get("mechanism_rationale"),
            },
            "fingerprint": fingerprint,
            "worker_note": f"exploration｜generated_candidate:{candidate_name}｜pool={exploration_pool}｜triage={triage_label}",
            "family_focus": proposal.get("target_family") or "generated",
        })
    return tasks
