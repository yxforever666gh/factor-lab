from __future__ import annotations

import json
from pathlib import Path
from typing import List

from factor_lab.analytics import evaluate_time_splits, factor_correlation_matrix
from factor_lab.data import SampleDataGenerator
from factor_lab.evaluation import evaluate_factor
from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.neutralization import neutralize_by_date
from factor_lab.portfolio import build_composite_factor, evaluate_long_short_portfolio
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

    portfolio_results = []
    composite_raw = build_composite_factor(dataset.frame, definitions, neutralize=False)
    portfolio_results.append(
        evaluate_long_short_portfolio(dataset.frame, composite_raw).to_dict()
    )
    if {"industry", "total_mv"}.issubset(dataset.frame.columns):
        composite_neutral = build_composite_factor(dataset.frame, definitions, neutralize=True)
        neutral_payload = evaluate_long_short_portfolio(dataset.frame, composite_neutral).to_dict()
        neutral_payload["strategy_name"] = "long_short_top_bottom_neutralized"
        portfolio_results.append(neutral_payload)

    with open(output / "portfolio_results.json", "w", encoding="utf-8") as handle:
        json.dump(portfolio_results, handle, ensure_ascii=False, indent=2)

    _write_summary(
        results=results,
        neutralized_results=neutralized_results,
        split_results=split_results,
        portfolio_results=portfolio_results,
        output_dir=output,
        source_name=config.get("data_source", "sample"),
    )
