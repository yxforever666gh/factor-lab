from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List

from factor_lab.analytics import (
    evaluate_rolling_windows,
    evaluate_time_splits,
    factor_correlation_matrix,
    high_correlation_peers,
    summarize_rolling_windows,
)
from factor_lab.clustering import greedy_correlation_clusters, pick_cluster_representatives
from factor_lab.data import SampleDataGenerator
from factor_lab.data_cache import ensure_feature_coverage, inspect_feature_store_coverage, slice_feature_store
from factor_lab.dedup import config_fingerprint
from factor_lab.evaluation import evaluate_factor
from factor_lab.experiments import ExperimentLedger
from factor_lab.factor_candidates import (
    build_hypothesis_summary,
    build_research_thesis_summary,
    derive_window_label,
    infer_factor_family,
    score_candidate_evaluation,
    summarize_candidate_status,
)
from factor_lab.candidate_graph import build_candidate_relationships
from factor_lab.factors import FactorDefinition, apply_factor, resolve_factor_definitions
from factor_lab.neutralization import neutralize_by_date
from factor_lab.portfolio import build_composite_factor, evaluate_long_short_portfolio
from factor_lab.registry import FactorRegistry
from factor_lab.scoring import score_factors
from factor_lab.storage import ExperimentStore
from factor_lab.robustness import refresh_candidate_risk_profiles
from factor_lab.tasks import TaskTracker
from factor_lab.timing import WorkflowTiming
from factor_lab.tushare_provider import TushareDataProvider
from factor_lab.universe import default_universe_name


def _load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_dataset(config: dict, timing: WorkflowTiming | None = None):
    source = config.get("data_source", "sample")
    if source == "tushare":
        provider = TushareDataProvider()
        cache_dir = config.get("cache_dir", "artifacts/tushare_cache")
        universe_limit = int(config.get("universe_limit", 80) or 80)
        research_profile = str(config.get("research_profile") or "").strip().lower()
        validation_mode = str(config.get("validation_mode") or "").strip().lower()
        allow_stale_cache_days = max(0, int(config.get("allow_stale_cache_days") or os.getenv("FACTOR_LAB_GENERATED_BATCH_ALLOW_STALE_CACHE_DAYS") or 0))
        should_defer_when_route_unhealthy = str(os.getenv("FACTOR_LAB_GENERATED_BATCH_DEFER_WHEN_ROUTE_UNHEALTHY") or "1").strip().lower() not in {"0", "false", "no", "off"}
        cheap_screen_mode = research_profile in {"opportunity_cheap_screen", "cheap_screen"} or validation_mode.startswith("light")

        def stale_cache_dataset(limit: int):
            if not cheap_screen_mode or allow_stale_cache_days <= 0:
                return None, None
            universe_name = config.get("universe_name") or default_universe_name(limit)
            coverage = inspect_feature_store_coverage(
                universe_name=universe_name,
                start_date=config["start_date"],
                end_date=config["end_date"],
                cache_dir=cache_dir,
            )
            stale_days = coverage.get("stale_days")
            effective_end_date = coverage.get("effective_end_date")
            if not coverage.get("available") or not coverage.get("covers_start") or stale_days is None:
                return None, universe_name
            if int(stale_days or 0) > allow_stale_cache_days or not effective_end_date:
                return None, universe_name
            dataset = slice_feature_store(
                universe_name=universe_name,
                start_date=config["start_date"],
                end_date=str(effective_end_date),
                cache_dir=cache_dir,
            )
            if dataset.frame.empty:
                return None, universe_name
            config["effective_end_date"] = str(effective_end_date)
            config["data_freshness"] = "stale_acceptable_for_cheap_screen"
            if timing:
                timing.set_counter("cache_hit_type", "feature_master_stale_fallback")
                timing.set_counter("stale_cache_days", int(stale_days or 0))
            return dataset, universe_name

        def load_for_limit(limit: int):
            universe_name = config.get("universe_name") or default_universe_name(limit)
            if should_defer_when_route_unhealthy and cheap_screen_mode and not provider.route_healthy():
                stale_dataset, stale_universe_name = stale_cache_dataset(limit)
                if stale_dataset is not None:
                    return stale_dataset, stale_universe_name or universe_name
                raise RuntimeError(
                    "tushare route unhealthy and no acceptable stale cache: "
                    f"universe={universe_name} start={config['start_date']} end={config['end_date']}"
                )
            ensure_feature_coverage(
                provider=provider,
                universe_limit=limit,
                start_date=config["start_date"],
                end_date=config["end_date"],
                cache_dir=cache_dir,
                universe_name=universe_name,
                timing=timing,
            )
            dataset = slice_feature_store(
                universe_name=universe_name,
                start_date=config["start_date"],
                end_date=config.get("effective_end_date") or config["end_date"],
                cache_dir=cache_dir,
            )
            return dataset, universe_name

        dataset, universe_name = load_for_limit(universe_limit)
        config["effective_universe_limit"] = universe_limit
        config["effective_universe_name"] = universe_name

        fallback_universe_limit = int(config.get("fallback_universe_limit") or 0)
        if dataset.frame.empty and fallback_universe_limit > universe_limit:
            fallback_dataset, fallback_universe_name = load_for_limit(fallback_universe_limit)
            if not fallback_dataset.frame.empty:
                dataset = fallback_dataset
                config["effective_universe_limit"] = fallback_universe_limit
                config["effective_universe_name"] = fallback_universe_name

        return dataset

    return SampleDataGenerator(seed=config.get("seed", 7)).generate(
        num_stocks=config.get("num_stocks", 60),
        num_days=config.get("num_days", 220),
    )


def _compute_factor_value(
    frame: pd.DataFrame,
    config: dict,
    factor_config_lookup: dict[str, dict],
    factor_value_cache: dict[str, pd.Series],
) -> pd.Series:
    name = config["name"]
    cached = factor_value_cache.get(name)
    if cached is not None:
        return cached

    operator = config.get("generator_operator")
    definition = FactorDefinition(name=name, expression=config["expression"])
    if not operator:
        values = apply_factor(frame, definition)
        factor_value_cache[name] = values
        return values

    left_name = config.get("left_factor_name")
    right_name = config.get("right_factor_name")
    left_config = factor_config_lookup.get(left_name) if left_name else None
    right_config = factor_config_lookup.get(right_name) if right_name else None
    left = _compute_factor_value(frame, left_config, factor_config_lookup, factor_value_cache) if left_config else factor_value_cache.get(left_name)
    right = _compute_factor_value(frame, right_config, factor_config_lookup, factor_value_cache) if right_config else factor_value_cache.get(right_name)

    # Arithmetic generators can still fall back to their compiled expression if lineage metadata is missing.
    if left is None or right is None:
        if operator not in {"residualize_against_peer", "orthogonalize_against_peer"}:
            values = apply_factor(frame, definition)
            factor_value_cache[name] = values
            return values
        raise KeyError(f"missing base factor for generated operator: {left_name}, {right_name}")

    if operator == "residualize_against_peer" or operator == "orthogonalize_against_peer":
        parts = []
        tmp = frame[["date"]].copy()
        tmp["left"] = left
        tmp["right"] = right
        for _, group in tmp.groupby("date", sort=True):
            subset = group[["left", "right"]].replace([float("inf"), float("-inf")], pd.NA).dropna()
            if subset.empty:
                parts.append(pd.Series(index=group.index, dtype=float))
                continue
            x = subset["right"].astype(float)
            y = subset["left"].astype(float)
            if x.nunique() <= 1:
                parts.append(pd.Series(y.values, index=subset.index))
                continue
            x_mean = float(x.mean())
            y_mean = float(y.mean())
            denom = float(((x - x_mean) ** 2).sum())
            beta = float((((x - x_mean) * (y - y_mean)).sum()) / denom) if denom else 0.0
            alpha = y_mean - beta * x_mean
            resid = y - (alpha + beta * x)
            parts.append(pd.Series(resid.values, index=subset.index))
        values = pd.concat(parts).sort_index().reindex(frame.index)
        factor_value_cache[name] = values
        return values

    values = apply_factor(frame, definition)
    factor_value_cache[name] = values
    return values


def _workflow_factor_eval_workers(definition_count: int) -> int:
    # Threading here is opt-in only. The current factor_eval path is pandas-heavy and
    # naive thread fan-out can be slower due to copy overhead and Python-level loops.
    raw = os.getenv("FACTOR_LAB_WORKFLOW_EVAL_WORKERS", "1").strip()
    try:
        value = int(raw)
    except Exception:
        value = 4
    return max(1, min(value, max(1, definition_count)))


def _evaluate_definition_bundle(
    dataset_frame: pd.DataFrame,
    definition: FactorDefinition,
    thresholds: dict[str, object],
    rolling_config: dict[str, object],
    factor_value_cache: dict[str, pd.Series],
) -> dict[str, object]:
    raw_factor = factor_value_cache[definition.name]
    factor_frame = dataset_frame[["date", "ticker", "forward_return_5d"]].copy()
    factor_frame["factor_value"] = raw_factor
    evaluation = evaluate_factor(
        frame=factor_frame,
        factor_name=definition.name,
        expression=definition.expression,
        thresholds=thresholds,
    )

    neutralized_payload = None
    if {"industry", "total_mv"}.issubset(dataset_frame.columns):
        neutralized_frame = dataset_frame[["date", "ticker", "forward_return_5d", "industry", "total_mv"]].copy()
        neutralized_frame["factor_value"] = neutralize_by_date(
            dataset_frame.assign(raw_factor=raw_factor),
            factor_col="raw_factor",
        )
        neutralized_eval = evaluate_factor(
            frame=neutralized_frame.dropna(subset=["factor_value"]),
            factor_name=definition.name,
            expression=definition.expression,
            thresholds=thresholds,
        )
        neutralized_payload = neutralized_eval.to_dict()
        neutralized_payload["variant"] = "neutralized"

    split_payload = evaluate_time_splits(
        frame=dataset_frame,
        definition=definition,
        thresholds=thresholds,
        evaluator=evaluate_factor,
        factor_values=raw_factor,
        factor_frame=factor_frame,
    )
    rolling_payload = evaluate_rolling_windows(
        frame=dataset_frame,
        definition=definition,
        thresholds=thresholds,
        evaluator=evaluate_factor,
        config=rolling_config,
        factor_values=raw_factor,
        factor_frame=factor_frame,
    )
    return {
        "result": evaluation.to_dict(),
        "neutralized": neutralized_payload,
        "splits": split_payload,
        "rolling": rolling_payload,
    }


def _write_summary(
    results: List[dict],
    neutralized_results: List[dict],
    split_results: List[dict],
    rolling_results: List[dict],
    rolling_summary_rows: List[dict],
    rolling_failures: List[dict],
    portfolio_results: List[dict],
    explore: List[dict],
    watchlist: List[dict],
    candidates: List[dict],
    graveyard: List[dict],
    scored_factors: List[dict],
    cluster_representatives: List[dict],
    output_dir: Path,
    source_name: str,
) -> None:
    passed = [r for r in results if r["pass_gate"]]
    failed = [r for r in results if not r["pass_gate"]]

    lines = [
        "# Workflow Summary",
        "",
        f"- Data source: {source_name}",
        f"- Total factors: {len(results)}",
        f"- Passed: {len(passed)}",
        f"- Failed: {len(failed)}",
        f"- Explore pool size: {len(explore)}",
        f"- Watchlist size: {len(watchlist)}",
        f"- Candidate pool size: {len(candidates)}",
        f"- Graveyard size: {len(graveyard)}",
        f"- Cluster representative count: {len(cluster_representatives)}",
        "",
        "## Main Results",
        "",
    ]

    for row in sorted(results, key=lambda item: item["rank_ic_mean"], reverse=True):
        status = "PASS" if row["pass_gate"] else "FAIL"
        lines.extend(
            [
                f"### {row['factor_name']} [{status}]",
                f"- Expression: `{row['expression']}`",
                f"- RankIC mean: {row['rank_ic_mean']}",
                f"- RankIC IR: {row['rank_ic_ir']}",
                f"- Top-bottom spread mean: {row['top_bottom_spread_mean']}",
                f"- Fail reason: {row['fail_reason'] or 'n/a'}",
                "",
            ]
        )

    if neutralized_results:
        lines.extend(["## Neutralized Results (industry + size)", ""])
        for row in sorted(neutralized_results, key=lambda item: item["rank_ic_mean"], reverse=True):
            status = "PASS" if row["pass_gate"] else "FAIL"
            lines.extend(
                [
                    f"- {row['factor_name']} [{status}]"
                    f" | RankIC={row['rank_ic_mean']} | IR={row['rank_ic_ir']}"
                    f" | Spread={row['top_bottom_spread_mean']}"
                    f" | Reason={row['fail_reason'] or 'n/a'}"
                ]
            )
        lines.append("")

    if split_results:
        lines.extend(["## Time Split Robustness", ""])
        for row in split_results:
            status = "PASS" if row["pass_gate"] else "FAIL"
            lines.extend(
                [
                    f"- {row['factor_name']} / {row['split']} [{status}]"
                    f" | RankIC={row['rank_ic_mean']} | Spread={row['top_bottom_spread_mean']}"
                    f" | Reason={row['fail_reason'] or 'n/a'}"
                ]
            )
        lines.append("")

    lines.extend(["## Factor Scores", ""])
    for row in scored_factors:
        lines.append(
            f"- {row['factor_name']} | score={row['score']} | rawScore={row.get('raw_score')} | neutralScore={row.get('neutral_score')} | rollingScore={row.get('rolling_score')} | corrPenalty={row.get('correlation_penalty')} | stylePenalty={row.get('style_exposure_penalty')} | peers={', '.join(row['high_corr_peers']) or 'none'}"
        )
    lines.append("")

    if rolling_summary_rows:
        lines.extend(["## Rolling Stability", ""])
        for row in rolling_summary_rows:
            lines.append(
                f"- {row['factor_name']} | windows={row['window_count']} | pass_rate={row['pass_rate']} | sign_flips={row['sign_flip_count']} | avgIC={row['avg_rank_ic_mean']} | ic_std={row['rank_ic_std']} | spread_std={row['spread_std']} | stability={row['stability_score']} | pass={row['pass_gate']}"
            )
        lines.append("")

    if rolling_failures:
        lines.extend(["## Rolling Failures", ""])
        for row in rolling_failures:
            lines.append(
                f"- {row['factor_name']} / {row['split']} | RankIC={row['rank_ic_mean']} | Spread={row['top_bottom_spread_mean']} | Reason={row['fail_reason'] or 'n/a'}"
            )
        lines.append("")

    lines.extend(["## Explore Pool", ""])
    if explore:
        for row in explore:
            lines.append(
                f"- {row['factor_name']} | role={row.get('factor_role')} | rawIC={row['raw_rank_ic_mean']} | rollingPass={row.get('rolling_pass')}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.extend(["## Watchlist", ""])
    if watchlist:
        for row in watchlist:
            lines.append(
                f"- {row['factor_name']} | role={row.get('factor_role')} | rawIC={row['raw_rank_ic_mean']} | neutralIC={row.get('neutralized_rank_ic_mean')} | rollingPass={row.get('rolling_pass')}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.extend(["## Candidate Pool", ""])
    if candidates:
        for row in candidates:
            lines.append(
                f"- {row['factor_name']} | rawIC={row['raw_rank_ic_mean']} | neutralIC={row['neutralized_rank_ic_mean']} | peers={', '.join(row['high_corr_peers']) or 'none'}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.extend(["## Cluster Representatives", ""])
    if cluster_representatives:
        for row in cluster_representatives:
            lines.append(
                f"- {row['factor_name']} | score={row['score']} | cluster={', '.join(row['cluster_members'])}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.extend(["## Graveyard", ""])
    if graveyard:
        for row in graveyard:
            lines.append(f"- {row['factor_name']} | reason={row['graveyard_reason']}")
    else:
        lines.append("- none")
    lines.append("")

    if portfolio_results:
        lines.extend(["## Portfolio Results", ""])
        for row in portfolio_results:
            lines.extend(
                [
                    f"### {row['strategy_name']}",
                    f"- Annual return: {row['annual_return']}",
                    f"- Annual volatility: {row['annual_volatility']}",
                    f"- Sharpe: {row['sharpe']}",
                    f"- Max drawdown: {row['max_drawdown']}",
                    f"- Avg turnover: {row['avg_turnover']}",
                    f"- Observations: {row['observations']}",
                    "",
                ]
            )

    (output_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")



def _build_rolling_outputs(rolling_results: list[dict], thresholds: dict | None = None) -> tuple[list[dict], list[dict]]:
    rolling_map: dict[str, list[dict]] = {}
    for row in rolling_results:
        rolling_map.setdefault(row['factor_name'], []).append(row)

    rolling_summary = []
    rolling_failures = []
    for factor_name, rows in sorted(rolling_map.items()):
        summary = summarize_rolling_windows(rows, thresholds)
        rolling_summary.append({'factor_name': factor_name, **summary})
        for row in rows:
            if not row.get('pass_gate'):
                rolling_failures.append(row)
    return rolling_summary, rolling_failures


def _register_candidate_intelligence(
    *,
    store: ExperimentStore,
    run_id: str,
    config_path: str,
    config: dict,
    results: list[dict],
    neutralized_results: list[dict],
    split_results: list[dict],
    rolling_results: list[dict],
    scored_factors: list[dict],
    candidates: list[dict],
    graveyard: list[dict],
    portfolio_results: list[dict],
    clusters: list[list[str]] | None = None,
    cluster_representatives: list[dict] | None = None,
) -> None:
    raw_map = {row['factor_name']: row for row in results}
    neutral_map = {row['factor_name']: row for row in neutralized_results}
    score_map = {row['factor_name']: row for row in scored_factors}
    split_map: dict[str, list[dict]] = {}
    for row in split_results:
        split_map.setdefault(row['factor_name'], []).append(row)
    rolling_map: dict[str, list[dict]] = {}
    for row in rolling_results:
        rolling_map.setdefault(row['factor_name'], []).append(row)
    candidate_map = {row['factor_name']: row for row in candidates}
    graveyard_map = {row['factor_name']: row for row in graveyard}

    representative_rows = list(cluster_representatives or [])
    representative_group_map: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in representative_rows:
        members = tuple(sorted(row.get('cluster_members') or [row.get('factor_name')]))
        group = representative_group_map.setdefault(
            members,
            {
                'cluster_members': list(members),
                'representative_candidates': [],
                'primary_candidate': None,
            },
        )
        factor_name = row.get('factor_name')
        if factor_name and factor_name not in group['representative_candidates']:
            group['representative_candidates'].append(factor_name)
        if row.get('is_primary_representative') and factor_name:
            group['primary_candidate'] = factor_name
    representative_context_by_name: dict[str, dict[str, Any]] = {}
    for row in representative_rows:
        members = tuple(sorted(row.get('cluster_members') or [row.get('factor_name')]))
        group = representative_group_map.get(members) or {}
        factor_name = row.get('factor_name')
        if not factor_name:
            continue
        representative_context_by_name[factor_name] = {
            'representative_candidate': factor_name,
            'representative_rank': row.get('cluster_rep_rank') or row.get('representative_rank'),
            'representative_count': row.get('cluster_rep_count') or row.get('representative_count'),
            'representative_candidates': list(group.get('representative_candidates') or [factor_name]),
            'primary_candidate': group.get('primary_candidate') or factor_name,
            'cluster_members': list(group.get('cluster_members') or [factor_name]),
        }
    for group in representative_group_map.values():
        primary_candidate = group.get('primary_candidate') or next(iter(group.get('representative_candidates') or group.get('cluster_members') or []), None)
        for member in group.get('cluster_members') or []:
            representative_context_by_name.setdefault(
                member,
                {
                    'representative_candidate': primary_candidate,
                    'representative_rank': None,
                    'representative_count': len(group.get('representative_candidates') or []),
                    'representative_candidates': list(group.get('representative_candidates') or []),
                    'primary_candidate': primary_candidate,
                    'cluster_members': list(group.get('cluster_members') or []),
                },
            )

    portfolio_by_name = {row['strategy_name']: row for row in portfolio_results}
    candidate_portfolio = portfolio_by_name.get('long_short_top_bottom_candidates_only') or {}
    all_portfolio = portfolio_by_name.get('long_short_top_bottom_all_factors') or {}
    coverage = 0.0
    if results:
        coverage = len(candidates) / max(len(results), 1)
    window_label = derive_window_label(config_path, config.get('start_date'), config.get('end_date'))
    candidate_id_by_name: dict[str, str] = {}
    family_by_name: dict[str, str] = {}

    factor_configs = resolve_factor_definitions(config, config_dir=Path(config_path).resolve().parent)
    for definition in factor_configs:
        name = definition['name']
        raw = raw_map.get(name, {})
        neutral = neutral_map.get(name, {})
        splits = split_map.get(name, [])
        score_row = score_map.get(name, {})
        rolling_summary = summarize_rolling_windows(rolling_map.get(name, []))
        robust_pass_count = int(rolling_summary.get('pass_count') or 0)
        robust_total_count = int(rolling_summary.get('window_count') or 0)
        candidate_payload = candidate_map.get(name) or graveyard_map.get(name) or {}
        inferred_family = infer_factor_family(name, definition.get('expression'))
        candidate_id = store.upsert_factor_candidate(
            name=name,
            family=inferred_family,
            definition=definition,
            expression=definition.get('expression'),
            origin_run_id=run_id,
            factor_role=definition.get('role'),
        )
        candidate_id_by_name[name] = candidate_id
        family_by_name[name] = inferred_family
        run_scope = 'official'
        if config.get('data_source') == 'sample' or 'first_workflow' in config_path:
            run_scope = 'demo'
        elif 'generated_' in config_path:
            run_scope = 'generated'
        elif 'tushare_batch' in config_path or 'batch' in config_path:
            run_scope = 'batch_official'

        metric_payload = {
            'sample_size': raw.get('observations') or 0,
            'observations': raw.get('observations') or 0,
            'return_metric': candidate_portfolio.get('annual_return') or all_portfolio.get('annual_return') or 0.0,
            'sharpe_like': candidate_portfolio.get('sharpe') or all_portfolio.get('sharpe') or 0.0,
            'max_drawdown': candidate_portfolio.get('max_drawdown') or all_portfolio.get('max_drawdown') or 0.0,
            'turnover': candidate_portfolio.get('avg_turnover') or all_portfolio.get('avg_turnover') or 0.0,
            'coverage': coverage,
            'raw_rank_ic_mean': raw.get('rank_ic_mean') or 0.0,
            'neutralized_rank_ic_mean': neutral.get('rank_ic_mean') or 0.0,
            'split_fail_count': candidate_payload.get('split_fail_count') or sum(1 for row in splits if not row.get('pass_gate')),
            'high_corr_peer_count': len(score_row.get('high_corr_peers') or []),
            'robust_pass_count': robust_pass_count,
            'robust_total_count': robust_total_count,
            'rolling_pass_rate': rolling_summary.get('pass_rate'),
            'rolling_sign_flip_count': rolling_summary.get('sign_flip_count'),
            'run_scope': run_scope,
        }
        scored = score_candidate_evaluation(metric_payload)
        notes = {
            'expression': definition.get('expression'),
            'factor_role': definition.get('role') or 'alpha_seed',
            'research_stage_hint': candidate_payload.get('research_stage'),
            'raw_pass': raw.get('pass_gate'),
            'neutralized_pass': neutral.get('pass_gate'),
            'high_corr_peers': score_row.get('high_corr_peers') or [],
            'source_run': run_id,
            'run_scope': run_scope,
            'config_path': config_path,
        }
        store.insert_factor_evaluation(
            {
                'candidate_id': candidate_id,
                'run_id': run_id,
                'window_label': window_label,
                'market_scope': config.get('effective_universe_name') or config.get('universe_name') or f"top_{config.get('effective_universe_limit') or config.get('universe_limit') or 'all'}",
                **metric_payload,
                **scored,
                'notes': notes,
                'created_at_utc': datetime.now(timezone.utc).isoformat(),
            }
        )
        evaluations = store.list_factor_evaluations(candidate_id=candidate_id, limit=200)
        summary = summarize_candidate_status(evaluations)
        store.refresh_factor_candidate(candidate_id, summary)
        candidate_row = store.get_factor_candidate(candidate_id) or {'name': name, 'family': inferred_family, 'status': summary.get('status')}
        hypothesis = build_hypothesis_summary(candidate_row, evaluations)
        store.upsert_research_hypothesis(candidate_id, hypothesis)
        thesis = build_research_thesis_summary(
            candidate_row,
            evaluations,
            representative_context=representative_context_by_name.get(name) or {},
        )
        store.upsert_research_thesis(candidate_id, thesis)

    relationship_rows = build_candidate_relationships(
        candidates=candidates,
        candidate_id_by_name=candidate_id_by_name,
        family_by_name=family_by_name,
        correlation_lookup={row['factor_name']: row.get('high_corr_peers') or [] for row in scored_factors},
        clusters=clusters or [],
        run_id=run_id,
    )
    for row in relationship_rows:
        store.upsert_candidate_relationship(
            left_candidate_id=row['left_candidate_id'],
            right_candidate_id=row['right_candidate_id'],
            relationship_type=row['relationship_type'],
            run_id=row.get('run_id'),
            strength=row.get('strength'),
            details=row.get('details'),
        )


def run_workflow(config_path: str, output_dir: str) -> None:
    timing = WorkflowTiming()
    with timing.stage("load_config"):
        config = _load_config(config_path)
    output = Path(output_dir or config.get("output_dir", "artifacts/workflow"))
    output.mkdir(parents=True, exist_ok=True)

    task_tracker = TaskTracker(output)
    task = task_tracker.start(config_path=config_path, output_dir=str(output))
    run_id = task["task_id"]
    created_at = task["started_at_utc"]
    cfg_fingerprint = config_fingerprint(config)

    try:
        store = ExperimentStore(Path("artifacts") / "factor_lab.db")
        latest_prior = store.find_latest_finished_run(cfg_fingerprint)

        task = task_tracker.update(task, stage="load_dataset")
        with timing.stage("load_dataset"):
            dataset = _load_dataset(config, timing=timing)
        if bool(config.get("write_dataset_csv", True)):
            dataset.frame.to_csv(output / "dataset.csv", index=False)
        if dataset.frame.empty:
            raise ValueError(
                "dataset slice empty: "
                f"start={config.get('start_date')} end={config.get('end_date')} "
                f"universe_limit={config.get('effective_universe_limit') or config.get('universe_limit')} output_dir={output}"
            )

        factor_configs = resolve_factor_definitions(config, config_dir=Path(config_path).resolve().parent)
        definitions = [FactorDefinition(name=item["name"], expression=item["expression"]) for item in factor_configs]
        factor_config_lookup = {item["name"]: item for item in factor_configs}
        timing.set_counter("factor_count", len(definitions))
        timing.set_counter("dataset_rows", int(len(dataset.frame)))
        results = []
        neutralized_results = []
        split_results = []
        rolling_results = []
        rolling_summary_rows = []
        rolling_failures = []
        rolling_config = config.get("rolling_validation") or {}
        thresholds = config.get("thresholds", {}) or {}
        factor_value_cache = {}
        for definition in definitions:
            factor_value_cache[definition.name] = _compute_factor_value(
                dataset.frame,
                factor_config_lookup.get(definition.name, {"name": definition.name, "expression": definition.expression}),
                factor_config_lookup,
                factor_value_cache,
            )
        task = task_tracker.update(task, stage="factor_eval")
        with timing.stage("factor_eval"):
            eval_workers = _workflow_factor_eval_workers(len(definitions))
            timing.set_counter("factor_eval_workers", eval_workers)
            if eval_workers == 1 or len(definitions) <= 1:
                bundles = [
                    _evaluate_definition_bundle(
                        dataset.frame,
                        definition,
                        thresholds,
                        rolling_config,
                        factor_value_cache,
                    )
                    for definition in definitions
                ]
            else:
                with ThreadPoolExecutor(max_workers=eval_workers) as executor:
                    bundles = list(
                        executor.map(
                            lambda definition: _evaluate_definition_bundle(
                                dataset.frame,
                                definition,
                                thresholds,
                                rolling_config,
                                factor_value_cache,
                            ),
                            definitions,
                        )
                    )
            for bundle in bundles:
                results.append(bundle["result"])
                if bundle.get("neutralized"):
                    neutralized_results.append(bundle["neutralized"])
                split_results.extend(bundle.get("splits") or [])
                rolling_results.extend(bundle.get("rolling") or [])

        task = task_tracker.update(task, stage="persist_intermediate")
        with timing.stage("persist_intermediate"):
            rolling_summary_rows, rolling_failures = _build_rolling_outputs(
                rolling_results,
                {**(config.get("thresholds", {}) or {}), **rolling_config},
            )
            with open(output / "results.json", "w", encoding="utf-8") as handle:
                json.dump(results, handle, ensure_ascii=False, indent=2)
            with open(output / "split_results.json", "w", encoding="utf-8") as handle:
                json.dump(split_results, handle, ensure_ascii=False, indent=2)
            with open(output / "rolling_results.json", "w", encoding="utf-8") as handle:
                json.dump(rolling_results, handle, ensure_ascii=False, indent=2)
            with open(output / "rolling_summary.json", "w", encoding="utf-8") as handle:
                json.dump(rolling_summary_rows, handle, ensure_ascii=False, indent=2)
            with open(output / "rolling_failures.json", "w", encoding="utf-8") as handle:
                json.dump(rolling_failures, handle, ensure_ascii=False, indent=2)
            with open(output / "neutralized_results.json", "w", encoding="utf-8") as handle:
                json.dump(neutralized_results, handle, ensure_ascii=False, indent=2)

        task = task_tracker.update(task, stage="correlation")
        with timing.stage("correlation"):
            correlation = factor_correlation_matrix(dataset.frame, definitions, factor_value_cache=factor_value_cache)
            correlation.to_csv(output / "factor_correlation.csv")
            corr_peers = high_correlation_peers(correlation, threshold=config.get("correlation_threshold", 0.8))
            scored_factors = score_factors(
                raw_results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                rolling_results=rolling_results,
                correlation_lookup=corr_peers,
                metadata_lookup={item['name']: item for item in factor_configs},
            )
            clusters = greedy_correlation_clusters(correlation, threshold=config.get("correlation_threshold", 0.8))
            cluster_representatives = pick_cluster_representatives(clusters, scored_factors)
            registry = FactorRegistry(output)
            metadata_lookup = {item["name"]: item for item in factor_configs}
            score_lookup = {item['factor_name']: item for item in scored_factors}
            explore, watchlist, candidates, graveyard = registry.build_candidate_and_graveyard(
                raw_results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                rolling_results=rolling_results,
                correlation_lookup=corr_peers,
                metadata_lookup=metadata_lookup,
                score_lookup=score_lookup,
            )
            registry.write_registry(explore, watchlist, candidates, graveyard, scored_factors, cluster_representatives)

        task = task_tracker.update(task, stage="portfolio")
        with timing.stage("portfolio"):
            portfolio_results = []
            cost_bps = float(config.get("portfolio_cost_bps_per_turnover") or 10.0)

            metadata_lookup = {item["name"]: item for item in factor_configs}
            allow_in_portfolio = {item["name"]: bool(item.get("allow_in_portfolio", True)) for item in factor_configs}
            cluster_names = {row["factor_name"] for row in cluster_representatives}
            candidate_names = {row["factor_name"] for row in candidates}
            watchlist_names = {row["factor_name"] for row in watchlist}

            def eval_group(strategy_name: str, defs, neutralize: bool = False):
                if not defs:
                    return
                signal = build_composite_factor(
                    dataset.frame,
                    defs,
                    neutralize=neutralize,
                    factor_value_cache=factor_value_cache,
                )
                payload = evaluate_long_short_portfolio(dataset.frame, signal, cost_bps_per_turnover=cost_bps).to_dict()
                payload["strategy_name"] = strategy_name
                portfolio_results.append(payload)

            # 1) baseline: all factors, control only
            eval_group("all_factors_baseline", definitions, neutralize=False)

            # 2) cluster representatives only, excluding exposure probes by default
            cluster_defs = [
                definition for definition in definitions
                if definition.name in cluster_names and metadata_lookup.get(definition.name, {}).get("role") != "exposure_probe" and allow_in_portfolio.get(definition.name, True)
            ]
            eval_group("cluster_representatives_only", cluster_defs, neutralize=False)

            # 3) neutralized survivors only: candidate + watchlist alpha/family probes
            neutral_survivor_defs = [
                definition for definition in definitions
                if definition.name in (candidate_names | watchlist_names)
                and metadata_lookup.get(definition.name, {}).get("role") != "exposure_probe"
                and allow_in_portfolio.get(definition.name, True)
            ]
            if {"industry", "total_mv"}.issubset(dataset.frame.columns):
                eval_group("neutralized_survivors_only", neutral_survivor_defs, neutralize=True)

            # 4) family distinct only: one non-exposure representative per family
            family_defs = []
            seen_families = set()
            for row in sorted(scored_factors, key=lambda item: item.get("score", -999), reverse=True):
                meta = metadata_lookup.get(row["factor_name"], {})
                family = meta.get("family") or infer_factor_family(row["factor_name"], row.get("expression"))
                if family in seen_families:
                    continue
                if meta.get("role") == "exposure_probe" or not allow_in_portfolio.get(row["factor_name"], True):
                    continue
                definition = next((d for d in definitions if d.name == row["factor_name"]), None)
                if definition is None:
                    continue
                family_defs.append(definition)
                seen_families.add(family)
            eval_group("family_distinct_only", family_defs, neutralize=False)

        task = task_tracker.update(task, stage="persist_final")
        with timing.stage("persist_final"):
            with open(output / "portfolio_results.json", "w", encoding="utf-8") as handle:
                json.dump(portfolio_results, handle, ensure_ascii=False, indent=2)

            ledger = ExperimentLedger(output)
            ledger_payload = {
                "run_id": run_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "config": config,
                "dataset_rows": int(len(dataset.frame)),
                "factor_count": len(definitions),
                "explore_pool": [row["factor_name"] for row in explore],
                "watchlist_pool": [row["factor_name"] for row in watchlist],
                "candidate_pool": [row["factor_name"] for row in candidates],
                "graveyard": [row["factor_name"] for row in graveyard],
                "cluster_representatives": [row["factor_name"] for row in cluster_representatives],
                "top_scores": scored_factors[:3],
                "portfolio_results": portfolio_results,
                "rolling_summary": rolling_summary_rows,
                "rolling_failures": rolling_failures,
            }
            ledger.write(ledger_payload)

            store.insert_run(
                {
                    "run_id": run_id,
                    "created_at_utc": created_at,
                    "config_path": config_path,
                    "output_dir": str(output),
                    "data_source": config.get("data_source", "sample"),
                    "start_date": config.get("start_date"),
                    "end_date": config.get("end_date"),
                    "universe_limit": config.get("effective_universe_limit") or config.get("universe_limit"),
                    "factor_count": len(definitions),
                    "dataset_rows": int(len(dataset.frame)),
                    "status": "finished",
                    "config_fingerprint": cfg_fingerprint,
                    "rerun_of_run_id": latest_prior[0] if latest_prior else None,
                }
            )

            split_fails = {}
            for row in split_results:
                split_fails.setdefault(row["factor_name"], 0)
                if not row["pass_gate"]:
                    split_fails[row["factor_name"]] += 1
            score_map = {row["factor_name"]: row for row in scored_factors}

            factor_rows = []
            for row in results:
                factor_rows.append(
                    {
                        "run_id": run_id,
                        "factor_name": row["factor_name"],
                        "variant": "raw_scored",
                        "expression": row["expression"],
                        "rank_ic_mean": row["rank_ic_mean"],
                        "rank_ic_ir": row["rank_ic_ir"],
                        "top_bottom_spread_mean": row["top_bottom_spread_mean"],
                        "pass_gate": row["pass_gate"],
                        "fail_reason": row["fail_reason"],
                        "score": score_map.get(row["factor_name"], {}).get("score"),
                        "split_fail_count": split_fails.get(row["factor_name"], 0),
                        "high_corr_peers": score_map.get(row["factor_name"], {}).get("high_corr_peers", []),
                    }
                )
            for row in neutralized_results:
                factor_rows.append(
                    {
                        "run_id": run_id,
                        "factor_name": row["factor_name"],
                        "variant": "neutralized",
                        "expression": row["expression"],
                        "rank_ic_mean": row["rank_ic_mean"],
                        "rank_ic_ir": row["rank_ic_ir"],
                        "top_bottom_spread_mean": row["top_bottom_spread_mean"],
                        "pass_gate": row["pass_gate"],
                        "fail_reason": row["fail_reason"],
                        "score": score_map.get(row["factor_name"], {}).get("score"),
                        "split_fail_count": split_fails.get(row["factor_name"], 0),
                        "high_corr_peers": score_map.get(row["factor_name"], {}).get("high_corr_peers", []),
                    }
                )
            for row in explore:
                factor_rows.append(
                    {
                        "run_id": run_id,
                        "factor_name": row["factor_name"],
                        "variant": "explore",
                        "expression": row["expression"],
                        "rank_ic_mean": row["raw_rank_ic_mean"],
                        "rank_ic_ir": row["raw_rank_ic_ir"],
                        "top_bottom_spread_mean": None,
                        "pass_gate": 1,
                        "fail_reason": None,
                        "score": score_map.get(row["factor_name"], {}).get("score"),
                        "split_fail_count": row.get("split_fail_count", 0),
                        "high_corr_peers": row.get("high_corr_peers", []),
                    }
                )
            for row in watchlist:
                factor_rows.append(
                    {
                        "run_id": run_id,
                        "factor_name": row["factor_name"],
                        "variant": "watchlist",
                        "expression": row["expression"],
                        "rank_ic_mean": row["raw_rank_ic_mean"],
                        "rank_ic_ir": row["raw_rank_ic_ir"],
                        "top_bottom_spread_mean": None,
                        "pass_gate": 1,
                        "fail_reason": None,
                        "score": score_map.get(row["factor_name"], {}).get("score"),
                        "split_fail_count": row.get("split_fail_count", 0),
                        "high_corr_peers": row.get("high_corr_peers", []),
                    }
                )
            for row in candidates:
                factor_rows.append(
                    {
                        "run_id": run_id,
                        "factor_name": row["factor_name"],
                        "variant": "candidate",
                        "expression": row["expression"],
                        "rank_ic_mean": row["raw_rank_ic_mean"],
                        "rank_ic_ir": row["raw_rank_ic_ir"],
                        "top_bottom_spread_mean": None,
                        "pass_gate": 1,
                        "fail_reason": None,
                        "score": score_map.get(row["factor_name"], {}).get("score"),
                        "split_fail_count": row.get("split_fail_count", 0),
                        "high_corr_peers": row.get("high_corr_peers", []),
                    }
                )
            for row in graveyard:
                factor_rows.append(
                    {
                        "run_id": run_id,
                        "factor_name": row["factor_name"],
                        "variant": "graveyard",
                        "expression": row["expression"],
                        "rank_ic_mean": row["raw_rank_ic_mean"],
                        "rank_ic_ir": row["raw_rank_ic_ir"],
                        "top_bottom_spread_mean": None,
                        "pass_gate": 0,
                        "fail_reason": row.get("graveyard_reason"),
                        "score": score_map.get(row["factor_name"], {}).get("score"),
                        "split_fail_count": row.get("split_fail_count", 0),
                        "high_corr_peers": row.get("high_corr_peers", []),
                    }
                )
            store.insert_factor_rows(factor_rows)
            store.insert_portfolio_rows(run_id, portfolio_results)
            _register_candidate_intelligence(
                store=store,
                run_id=run_id,
                config_path=config_path,
                config=config,
                results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                rolling_results=rolling_results,
                scored_factors=scored_factors,
                candidates=candidates,
                graveyard=graveyard,
                portfolio_results=portfolio_results,
                clusters=clusters,
                cluster_representatives=cluster_representatives,
            )
            if bool(config.get("refresh_global_risk", True)):
                refresh_candidate_risk_profiles(store, run_id=run_id, output_dir=Path("artifacts"))

            # Exposure Track: strength-first factors for style/industry rotation.
            if bool(config.get("refresh_exposure_track", True)):
                try:
                    from factor_lab.exposure_track import refresh_exposure_track

                    refresh_exposure_track(store, run_id=run_id)
                except Exception:
                    # Exposure track should not break the main workflow.
                    pass

            _write_summary(
                results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                rolling_results=rolling_results,
                rolling_summary_rows=rolling_summary_rows,
                rolling_failures=rolling_failures,
                portfolio_results=portfolio_results,
                explore=explore,
                watchlist=watchlist,
                candidates=candidates,
                graveyard=graveyard,
                scored_factors=scored_factors,
                cluster_representatives=cluster_representatives,
                output_dir=output,
                source_name=config.get("data_source", "sample"),
            )
            timing.write_json(output / "timing.json")
            store.insert_artifacts(
                run_id,
                [
                    ("summary", str(output / "summary.md")),
                    ("ledger", str(output / "experiment_ledger.json")),
                    ("scores", str(output / "factor_scores.json")),
                    ("rolling", str(output / "rolling_results.json")),
                    ("rolling_summary", str(output / "rolling_summary.json")),
                    ("rolling_failures", str(output / "rolling_failures.json")),
                    ("explore", str(output / "explore_pool.json")),
                    ("watchlist", str(output / "watchlist_pool.json")),
                    ("candidate_status_snapshot", str(output / "candidate_status_snapshot.json")),
                    ("portfolio", str(output / "portfolio_results.json")),
                    ("timing", str(output / "timing.json")),
                ],
            )
        task_tracker.finish(task, status="finished")
    except Exception as exc:
        timing.write_json(output / "timing.json")
        task_tracker.finish(task, status="failed", error=str(exc))
        store = ExperimentStore(Path("artifacts") / "factor_lab.db")
        store.insert_run(
            {
                "run_id": run_id,
                "created_at_utc": created_at,
                "config_path": config_path,
                "output_dir": str(output),
                "data_source": config.get("data_source", "sample"),
                "start_date": config.get("start_date"),
                "end_date": config.get("end_date"),
                "universe_limit": config.get("effective_universe_limit") or config.get("universe_limit"),
                "factor_count": len(config.get("factors", [])),
                "dataset_rows": 0,
                "status": "failed",
                "config_fingerprint": cfg_fingerprint,
                "rerun_of_run_id": None,
            }
        )
        raise
