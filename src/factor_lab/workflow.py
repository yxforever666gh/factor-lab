from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd

from factor_lab.data import SampleDataGenerator
from factor_lab.evaluation import evaluate_factor
from factor_lab.factors import FactorDefinition, apply_factor


def _load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_summary(results: List[dict], output_dir: Path) -> None:
    passed = [r for r in results if r["pass_gate"]]
    failed = [r for r in results if not r["pass_gate"]]

    lines = [
        "# First Workflow Summary",
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

    (output_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def run_workflow(config_path: str, output_dir: str) -> None:
    config = _load_config(config_path)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    dataset = SampleDataGenerator(seed=config.get("seed", 7)).generate(
        num_stocks=config.get("num_stocks", 60),
        num_days=config.get("num_days", 220),
    )

    dataset.frame.to_csv(output / "sample_dataset.csv", index=False)

    results = []
    for item in config["factors"]:
        definition = FactorDefinition(name=item["name"], expression=item["expression"])
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

    with open(output / "results.json", "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2)

    _write_summary(results=results, output_dir=output)
