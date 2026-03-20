from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from factor_lab.analytics import evaluate_time_splits, factor_correlation_matrix, high_correlation_peers
from factor_lab.clustering import greedy_correlation_clusters, pick_cluster_representatives
from factor_lab.data import SampleDataGenerator
from factor_lab.data_cache import ensure_feature_coverage, slice_feature_store
from factor_lab.dedup import config_fingerprint
from factor_lab.evaluation import evaluate_factor
from factor_lab.experiments import ExperimentLedger
from factor_lab.factor_candidates import (
    build_hypothesis_summary,
    derive_window_label,
    infer_factor_family,
    score_candidate_evaluation,
    summarize_candidate_status,
)
from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.neutralization import neutralize_by_date
from factor_lab.portfolio import build_composite_factor, evaluate_long_short_portfolio
from factor_lab.registry import FactorRegistry
from factor_lab.scoring import score_factors
from factor_lab.storage import ExperimentStore
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
        universe_limit = config.get("universe_limit", 80)
        universe_name = config.get("universe_name") or default_universe_name(universe_limit)
        ensure_feature_coverage(
            provider=provider,
            universe_limit=universe_limit,
            start_date=config["start_date"],
            end_date=config["end_date"],
            cache_dir=cache_dir,
            universe_name=universe_name,
            timing=timing,
        )
        return slice_feature_store(
            universe_name=universe_name,
            start_date=config["start_date"],
            end_date=config["end_date"],
            cache_dir=cache_dir,
        )

    return SampleDataGenerator(seed=config.get("seed", 7)).generate(
        num_stocks=config.get("num_stocks", 60),
        num_days=config.get("num_days", 220),
    )


def _write_summary(
    results: List[dict],
    neutralized_results: List[dict],
    split_results: List[dict],
    portfolio_results: List[dict],
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
            f"- {row['factor_name']} | score={row['score']} | rawIC={row['raw_rank_ic_mean']} | neutralIC={row['neutralized_rank_ic_mean']} | peers={', '.join(row['high_corr_peers']) or 'none'}"
        )
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


def _register_candidate_intelligence(
    *,
    store: ExperimentStore,
    run_id: str,
    config_path: str,
    config: dict,
    results: list[dict],
    neutralized_results: list[dict],
    split_results: list[dict],
    scored_factors: list[dict],
    candidates: list[dict],
    graveyard: list[dict],
    portfolio_results: list[dict],
) -> None:
    raw_map = {row['factor_name']: row for row in results}
    neutral_map = {row['factor_name']: row for row in neutralized_results}
    score_map = {row['factor_name']: row for row in scored_factors}
    split_map: dict[str, list[dict]] = {}
    for row in split_results:
        split_map.setdefault(row['factor_name'], []).append(row)
    candidate_map = {row['factor_name']: row for row in candidates}
    graveyard_map = {row['factor_name']: row for row in graveyard}

    portfolio_by_name = {row['strategy_name']: row for row in portfolio_results}
    candidate_portfolio = portfolio_by_name.get('long_short_top_bottom_candidates_only') or {}
    all_portfolio = portfolio_by_name.get('long_short_top_bottom_all_factors') or {}
    coverage = 0.0
    if results:
        coverage = len(candidates) / max(len(results), 1)
    window_label = derive_window_label(config_path, config.get('start_date'), config.get('end_date'))

    for definition in config['factors']:
        name = definition['name']
        raw = raw_map.get(name, {})
        neutral = neutral_map.get(name, {})
        splits = split_map.get(name, [])
        score_row = score_map.get(name, {})
        robust_pass_count = sum(1 for row in splits if row.get('pass_gate'))
        robust_total_count = len(splits)
        candidate_payload = candidate_map.get(name) or graveyard_map.get(name) or {}
        candidate_id = store.upsert_factor_candidate(
            name=name,
            family=infer_factor_family(name, definition.get('expression')),
            definition=definition,
            expression=definition.get('expression'),
            origin_run_id=run_id,
        )
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
            'run_scope': run_scope,
        }
        scored = score_candidate_evaluation(metric_payload)
        notes = {
            'expression': definition.get('expression'),
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
                'market_scope': config.get('universe_name') or f"top_{config.get('universe_limit') or 'all'}",
                **metric_payload,
                **scored,
                'notes': notes,
                'created_at_utc': datetime.now(timezone.utc).isoformat(),
            }
        )
        evaluations = store.list_factor_evaluations(candidate_id=candidate_id, limit=200)
        summary = summarize_candidate_status(evaluations)
        store.refresh_factor_candidate(candidate_id, summary)
        candidate_row = store.get_factor_candidate(candidate_id) or {'name': name, 'family': infer_factor_family(name, definition.get('expression')), 'status': summary.get('status')}
        hypothesis = build_hypothesis_summary(candidate_row, evaluations)
        store.upsert_research_hypothesis(candidate_id, hypothesis)


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

        with timing.stage("load_dataset"):
            dataset = _load_dataset(config, timing=timing)
        dataset.frame.to_csv(output / "dataset.csv", index=False)

        definitions = [FactorDefinition(name=item["name"], expression=item["expression"]) for item in config["factors"]]
        timing.set_counter("factor_count", len(definitions))
        timing.set_counter("dataset_rows", int(len(dataset.frame)))

        results = []
        neutralized_results = []
        split_results = []
        with timing.stage("factor_eval"):
            for definition in definitions:
                factor_frame = dataset.frame.copy()
                raw_factor = apply_factor(dataset.frame, definition)
                factor_frame["factor_value"] = raw_factor
                evaluation = evaluate_factor(
                    frame=factor_frame,
                    factor_name=definition.name,
                    expression=definition.expression,
                    thresholds=config.get("thresholds", {}),
                )
                results.append(evaluation.to_dict())

                if {"industry", "total_mv"}.issubset(dataset.frame.columns):
                    neutralized_frame = dataset.frame.copy()
                    neutralized_frame["factor_value"] = neutralize_by_date(
                        factor_frame.assign(raw_factor=raw_factor),
                        factor_col="raw_factor",
                    )
                    neutralized_eval = evaluate_factor(
                        frame=neutralized_frame.dropna(subset=["factor_value"]),
                        factor_name=definition.name,
                        expression=definition.expression,
                        thresholds=config.get("thresholds", {}),
                    )
                    payload = neutralized_eval.to_dict()
                    payload["variant"] = "neutralized"
                    neutralized_results.append(payload)

                split_results.extend(
                    evaluate_time_splits(
                        frame=dataset.frame,
                        definition=definition,
                        thresholds=config.get("thresholds", {}),
                        evaluator=evaluate_factor,
                    )
                )

        with timing.stage("persist_intermediate"):
            with open(output / "results.json", "w", encoding="utf-8") as handle:
                json.dump(results, handle, ensure_ascii=False, indent=2)
            with open(output / "split_results.json", "w", encoding="utf-8") as handle:
                json.dump(split_results, handle, ensure_ascii=False, indent=2)
            with open(output / "neutralized_results.json", "w", encoding="utf-8") as handle:
                json.dump(neutralized_results, handle, ensure_ascii=False, indent=2)

        with timing.stage("correlation"):
            correlation = factor_correlation_matrix(dataset.frame, definitions)
            correlation.to_csv(output / "factor_correlation.csv")
            corr_peers = high_correlation_peers(correlation, threshold=config.get("correlation_threshold", 0.8))
            scored_factors = score_factors(
                raw_results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                correlation_lookup=corr_peers,
            )
            clusters = greedy_correlation_clusters(correlation, threshold=config.get("correlation_threshold", 0.8))
            cluster_representatives = pick_cluster_representatives(clusters, scored_factors)
            registry = FactorRegistry(output)
            candidates, graveyard = registry.build_candidate_and_graveyard(
                raw_results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                correlation_lookup=corr_peers,
            )
            registry.write_registry(candidates, graveyard, scored_factors, cluster_representatives)

        with timing.stage("portfolio"):
            portfolio_results = []
            composite_raw = build_composite_factor(dataset.frame, definitions, neutralize=False)
            raw_payload = evaluate_long_short_portfolio(dataset.frame, composite_raw).to_dict()
            raw_payload["strategy_name"] = "long_short_top_bottom_all_factors"
            portfolio_results.append(raw_payload)

            candidate_defs = [definition for definition in definitions if any(c["factor_name"] == definition.name for c in candidates)]
            if candidate_defs:
                candidate_signal = build_composite_factor(dataset.frame, candidate_defs, neutralize=False)
                candidate_payload = evaluate_long_short_portfolio(dataset.frame, candidate_signal).to_dict()
                candidate_payload["strategy_name"] = "long_short_top_bottom_candidates_only"
                portfolio_results.append(candidate_payload)

                if {"industry", "total_mv"}.issubset(dataset.frame.columns):
                    candidate_neutral_signal = build_composite_factor(dataset.frame, candidate_defs, neutralize=True)
                    candidate_neutral_payload = evaluate_long_short_portfolio(dataset.frame, candidate_neutral_signal).to_dict()
                    candidate_neutral_payload["strategy_name"] = "long_short_top_bottom_candidates_only_neutralized"
                    portfolio_results.append(candidate_neutral_payload)

            rep_defs = [definition for definition in definitions if any(c["factor_name"] == definition.name for c in cluster_representatives)]
            if rep_defs:
                rep_signal = build_composite_factor(dataset.frame, rep_defs, neutralize=False)
                rep_payload = evaluate_long_short_portfolio(dataset.frame, rep_signal).to_dict()
                rep_payload["strategy_name"] = "long_short_top_bottom_cluster_representatives"
                portfolio_results.append(rep_payload)

                if {"industry", "total_mv"}.issubset(dataset.frame.columns):
                    rep_neutral_signal = build_composite_factor(dataset.frame, rep_defs, neutralize=True)
                    rep_neutral_payload = evaluate_long_short_portfolio(dataset.frame, rep_neutral_signal).to_dict()
                    rep_neutral_payload["strategy_name"] = "long_short_top_bottom_cluster_representatives_neutralized"
                    portfolio_results.append(rep_neutral_payload)

            if {"industry", "total_mv"}.issubset(dataset.frame.columns):
                composite_neutral = build_composite_factor(dataset.frame, definitions, neutralize=True)
                neutral_payload = evaluate_long_short_portfolio(dataset.frame, composite_neutral).to_dict()
                neutral_payload["strategy_name"] = "long_short_top_bottom_neutralized"
                portfolio_results.append(neutral_payload)

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
                "candidate_pool": [row["factor_name"] for row in candidates],
                "graveyard": [row["factor_name"] for row in graveyard],
                "cluster_representatives": [row["factor_name"] for row in cluster_representatives],
                "top_scores": scored_factors[:3],
                "portfolio_results": portfolio_results,
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
                    "universe_limit": config.get("universe_limit"),
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
                scored_factors=scored_factors,
                candidates=candidates,
                graveyard=graveyard,
                portfolio_results=portfolio_results,
            )

            _write_summary(
                results=results,
                neutralized_results=neutralized_results,
                split_results=split_results,
                portfolio_results=portfolio_results,
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
                "universe_limit": config.get("universe_limit"),
                "factor_count": len(config.get("factors", [])),
                "dataset_rows": 0,
                "status": "failed",
                "config_fingerprint": cfg_fingerprint,
                "rerun_of_run_id": None,
            }
        )
        raise
