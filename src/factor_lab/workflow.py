from __future__ import annotations

import json
from pathlib import Path
from typing import List

from factor_lab.analytics import factor_correlation_matrix, evaluate_time_splits
from factor_lab.data import SampleDataGenerator
from factor_lab.evaluation import evaluate_factor
from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.tushare_provider import TushareDataProvider, TushareRequest


def _load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_dataset(config: dict):
    source = config.get("data_source", "sample")
    if source == "sample":
        return SampleDataGenerator(seed=config.get("seed", 7)).generate(
            num_stocks=config.get("num_stocks", 60),
            num_days=config.get("num_days", 220),
        )
    if source == "tushare":
        provider = TushareDataProvider()
        request = TushareRequest(
            start_date=config["start_date"],
            end_date=config["end_date"],
            universe_limit=config.get("universe_limit", 80),
            cache_dir=config.get("cache_dir", "artifacts/tushare_cache"),
        )
        return provider.load_dataset(request)
    raise ValueError(f"Unsupported data source: {source}")


def _write_summary(results: List[dict], split_results: List[dict], output_dir: Path) -> None:
    passed = [r for r in results if r["pass_gate"]]
    failed = [r for r in results if not r["pass_gate"]]

    lines = [
        "# Workflow Summary",
        "",
        f"- Total factors: {len(results)}",
        f"- Passed: {len(passed)}",
        f"- Failed: {len(failed)}",
        "",
        "## Results",
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

    if split_results:
        lines.extend(["## Time Split Robustness", ""])
        for row in split_results:
            status = "PASS" if row["pass_gate"] else "FAIL"
            lines.extend(
                [
                    f"- {row['factor_name']} / {row['split']}: {status}, RankIC={row['rank_ic_mean']}, Spread={row['top_bottom_spread_mean']}",
                ]
            )
        lines.append("")

    (output_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_workflow(config_path: str, output_dir: str) -> None:
    config = _load_config(config_path)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    dataset = _load_dataset(config)
    dataset.frame.to_csv(output / "dataset.csv", index=False)

    results = []
    split_results = []
    definitions = []

    for item in config["factors"]:
        definition = FactorDefinition(name=item["name"], expression=item["expression"])
        definitions.append(definition)
        factor_values = apply_factor(dataset.frame, definition)
        factor_frame = dataset.frame.copy()
        factor_frame["factor_value"] = factor_values
        evaluation = evaluate_factor(
            frame=factor_frame,
            factor_name=definition.name,
            expression=definition.expression,
            thresholds=config.get("thresholds", {}),
        )
        results.append(evaluation.to_dict())
        split_results.extend(
            evaluate_time_splits(
                frame=dataset.frame,
                definition=definition,
                thresholds=config.get("thresholds", {}),
                evaluator=evaluate_factor,
            )
        )

    corr = factor_correlation_matrix(dataset.frame, definitions)
    corr.to_csv(output / "factor_correlation.csv")

    with open(output / "results.json", "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2)

    with open(output / "split_results.json", "w", encoding="utf-8") as handle:
        json.dump(split_results, handle, ensure_ascii=False, indent=2)

    _write_summary(results=results, split_results=split_results, output_dir=output)
