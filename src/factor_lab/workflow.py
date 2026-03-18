from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from factor_lab.analytics import evaluate_time_splits, factor_correlation_matrix, high_correlation_peers
from factor_lab.clustering import greedy_correlation_clusters, pick_cluster_representatives
from factor_lab.data import SampleDataGenerator
from factor_lab.evaluation import evaluate_factor
from factor_lab.experiments import ExperimentLedger
from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.neutralization import neutralize_by_date
from factor_lab.portfolio import build_composite_factor, evaluate_long_short_portfolio
from factor_lab.registry import FactorRegistry
from factor_lab.scoring import score_factors
from factor_lab.tushare_provider import TushareDataProvider, TushareRequest


def _load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_dataset(config: dict):
    source = config.get("data_source", "sample")
    if source == "tushare":
        provider = TushareDataProvider()
        return provider.load_dataset(
            TushareRequest(
                start_date=config["start_date"],
                end_date=config["end_date"],
                universe_limit=config.get("universe_limit", 80),
                cache_dir=config.get("cache_dir", "artifacts/tushare_cache"),
            )
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


def run_workflow(config_path: str, output_dir: str) -> None:
    config = _load_config(config_path)
    output = Path(output_dir or config.get("output_dir", "artifacts/workflow"))
    output.mkdir(parents=True, exist_ok=True)

    dataset = _load_dataset(config)
    dataset.frame.to_csv(output / "dataset.csv", index=False)

    definitions = [FactorDefinition(name=item["name"], expression=item["expression"]) for item in config["factors"]]

    results = []
    neutralized_results = []
    split_results = []
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

    with open(output / "results.json", "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2)

    with open(output / "split_results.json", "w", encoding="utf-8") as handle:
        json.dump(split_results, handle, ensure_ascii=False, indent=2)

    with open(output / "neutralized_results.json", "w", encoding="utf-8") as handle:
        json.dump(neutralized_results, handle, ensure_ascii=False, indent=2)

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

    with open(output / "portfolio_results.json", "w", encoding="utf-8") as handle:
        json.dump(portfolio_results, handle, ensure_ascii=False, indent=2)

    ledger = ExperimentLedger(output)
    ledger.write(
        {
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
