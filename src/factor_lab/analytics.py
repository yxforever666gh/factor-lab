from __future__ import annotations

from typing import Dict, List

import pandas as pd

from factor_lab.factors import FactorDefinition, apply_factor


def build_factor_value_frame(frame: pd.DataFrame, definitions: List[FactorDefinition]) -> pd.DataFrame:
    base = frame[["date", "ticker", "forward_return_5d"]].copy()
    for definition in definitions:
        base[definition.name] = apply_factor(frame, definition)
    return base


def factor_correlation_matrix(frame: pd.DataFrame, definitions: List[FactorDefinition]) -> pd.DataFrame:
    values = {definition.name: apply_factor(frame, definition) for definition in definitions}
    factor_df = pd.DataFrame(values)
    return factor_df.corr(method="spearman")


def evaluate_time_splits(frame: pd.DataFrame, definition: FactorDefinition, thresholds: dict, evaluator) -> List[Dict]:
    dates = sorted(frame["date"].drop_duplicates())
    if len(dates) < 6:
        return []
    midpoint = len(dates) // 2
    splits = {
        "first_half": set(dates[:midpoint]),
        "second_half": set(dates[midpoint:]),
    }
    results = []
    factor_values = apply_factor(frame, definition)
    factor_frame = frame.copy()
    factor_frame["factor_value"] = factor_values
    for label, allowed_dates in splits.items():
        subset = factor_frame[factor_frame["date"].isin(allowed_dates)].copy()
        if subset.empty:
            continue
        evaluation = evaluator(
            frame=subset,
            factor_name=definition.name,
            expression=definition.expression,
            thresholds=thresholds,
        )
        payload = evaluation.to_dict()
        payload["split"] = label
        results.append(payload)
    return results
