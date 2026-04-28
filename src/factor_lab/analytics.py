from __future__ import annotations

from math import ceil
from statistics import pstdev
from typing import Any, Dict, List

import pandas as pd

from factor_lab.factors import FactorDefinition, apply_factor


def build_factor_value_frame(frame: pd.DataFrame, definitions: List[FactorDefinition]) -> pd.DataFrame:
    base = frame[["date", "ticker", "forward_return_5d"]].copy()
    for definition in definitions:
        base[definition.name] = apply_factor(frame, definition)
    return base


def factor_correlation_matrix(
    frame: pd.DataFrame | None = None,
    definitions: List[FactorDefinition] | None = None,
    *,
    factor_value_cache: Dict[str, pd.Series] | None = None,
    factor_value_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if factor_value_frame is not None:
        factor_df = factor_value_frame.drop(columns=[col for col in ["date", "ticker", "forward_return_5d"] if col in factor_value_frame.columns])
        return factor_df.corr(method="spearman")
    if factor_value_cache is not None:
        factor_df = pd.DataFrame({name: series for name, series in factor_value_cache.items()})
        return factor_df.corr(method="spearman")
    values = {definition.name: apply_factor(frame, definition) for definition in (definitions or [])}
    factor_df = pd.DataFrame(values)
    return factor_df.corr(method="spearman")


def high_correlation_peers(correlation: pd.DataFrame, threshold: float = 0.8) -> Dict[str, List[str]]:
    peers: Dict[str, List[str]] = {}
    for col in correlation.columns:
        matches = []
        for idx, value in correlation[col].items():
            if idx == col:
                continue
            if pd.notna(value) and abs(float(value)) >= threshold:
                matches.append(idx)
        peers[col] = sorted(matches)
    return peers


def evaluate_time_splits(
    frame: pd.DataFrame,
    definition: FactorDefinition,
    thresholds: dict,
    evaluator,
    *,
    factor_values: pd.Series | None = None,
    factor_frame: pd.DataFrame | None = None,
) -> List[Dict]:
    if factor_frame is None:
        factor_frame = frame[["date", "ticker", "forward_return_5d"]].copy()
        factor_frame["factor_value"] = factor_values if factor_values is not None else apply_factor(frame, definition)
    dates = factor_frame["date"].drop_duplicates().sort_values().tolist()
    if len(dates) < 6:
        return []
    midpoint = len(dates) // 2
    date_rank = {date: idx for idx, date in enumerate(dates)}
    frame_with_idx = factor_frame.assign(_date_idx=factor_frame["date"].map(date_rank))
    split_ranges = {
        "first_half": (0, midpoint),
        "second_half": (midpoint, len(dates)),
    }
    results = []
    for label, (start_idx, end_idx) in split_ranges.items():
        subset = frame_with_idx[(frame_with_idx["_date_idx"] >= start_idx) & (frame_with_idx["_date_idx"] < end_idx)].drop(columns=["_date_idx"])
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


def evaluate_rolling_windows(
    frame: pd.DataFrame,
    definition: FactorDefinition,
    thresholds: dict,
    evaluator,
    config: dict[str, Any] | None = None,
    *,
    factor_values: pd.Series | None = None,
    factor_frame: pd.DataFrame | None = None,
) -> List[Dict[str, Any]]:
    config = config or {}
    if factor_frame is None:
        factor_frame = frame[["date", "ticker", "forward_return_5d"]].copy()
        factor_frame["factor_value"] = factor_values if factor_values is not None else apply_factor(frame, definition)
    dates = factor_frame["date"].drop_duplicates().sort_values().tolist()
    if len(dates) < 12:
        return []

    requested_window = int(config.get("window_size") or 0)
    requested_step = int(config.get("step_size") or 0)
    window_size = requested_window or max(12, len(dates) // 3)
    window_size = min(window_size, len(dates))
    if window_size < 8:
        return []
    step_size = requested_step or max(4, window_size // 2)

    results: List[Dict[str, Any]] = []
    start_indexes = list(range(0, max(len(dates) - window_size + 1, 1), step_size))
    last_possible = len(dates) - window_size
    if last_possible >= 0 and (not start_indexes or start_indexes[-1] != last_possible):
        start_indexes.append(last_possible)

    date_rank = {date: idx for idx, date in enumerate(dates)}
    frame_with_idx = factor_frame.assign(_date_idx=factor_frame["date"].map(date_rank))

    for idx, start in enumerate(start_indexes, start=1):
        end = start + window_size
        subset = frame_with_idx[(frame_with_idx["_date_idx"] >= start) & (frame_with_idx["_date_idx"] < end)].drop(columns=["_date_idx"])
        if subset.empty:
            continue
        evaluation = evaluator(
            frame=subset,
            factor_name=definition.name,
            expression=definition.expression,
            thresholds=thresholds,
        )
        payload = evaluation.to_dict()
        payload["split"] = f"rolling_{idx:02d}"
        payload["window_index"] = idx
        payload["window_start_date"] = str(dates[start].date())
        payload["window_end_date"] = str(dates[end - 1].date())
        payload["window_size"] = int(window_size)
        results.append(payload)
    return results


def summarize_rolling_windows(
    rolling_results: List[Dict[str, Any]],
    thresholds: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    thresholds = thresholds or {}
    if not rolling_results:
        return {
            "window_count": 0,
            "pass_count": 0,
            "fail_count": 0,
            "pass_rate": None,
            "positive_rank_ic_ratio": None,
            "positive_spread_ratio": None,
            "sign_flip_count": 0,
            "avg_rank_ic_mean": None,
            "avg_top_bottom_spread_mean": None,
            "rank_ic_std": None,
            "spread_std": None,
            "worst_rank_ic_mean": None,
            "worst_top_bottom_spread_mean": None,
            "stability_score": None,
            "pass_gate": False,
        }

    rank_ics = [float(row.get("rank_ic_mean") or 0.0) for row in rolling_results]
    spreads = [float(row.get("top_bottom_spread_mean") or 0.0) for row in rolling_results]
    pass_count = sum(1 for row in rolling_results if row.get("pass_gate"))
    sign_flip_count = 0
    prev_sign = 0
    for value in rank_ics:
        sign = 1 if value > 0 else (-1 if value < 0 else 0)
        if sign and prev_sign and sign != prev_sign:
            sign_flip_count += 1
        if sign:
            prev_sign = sign

    window_count = len(rolling_results)
    avg_rank_ic = sum(rank_ics) / window_count
    avg_spread = sum(spreads) / window_count
    pass_rate = pass_count / window_count
    positive_rank_ic_ratio = sum(1 for value in rank_ics if value > 0) / window_count
    positive_spread_ratio = sum(1 for value in spreads if value > 0) / window_count
    rank_ic_std = pstdev(rank_ics) if window_count > 1 else 0.0
    spread_std = pstdev(spreads) if window_count > 1 else 0.0

    min_pass_rate = float(thresholds.get("min_pass_rate") or 0.6)
    max_sign_flips = int(thresholds.get("max_sign_flips") or max(1, ceil(window_count / 3)))
    pass_gate = (
        pass_rate >= min_pass_rate
        and sign_flip_count <= max_sign_flips
        and avg_rank_ic >= float(thresholds.get("min_rank_ic") or 0.0)
    )

    rank_component = min(max(avg_rank_ic / max(float(thresholds.get("min_rank_ic") or 0.01), 1e-6), 0.0), 2.0) / 2.0
    spread_floor = max(float(thresholds.get("min_top_bottom_spread") or 0.001), 1e-6)
    spread_component = min(max(avg_spread / spread_floor, 0.0), 2.0) / 2.0
    std_penalty = min(rank_ic_std / max(abs(avg_rank_ic), 0.01), 2.0) * 0.08 + min(spread_std / spread_floor, 2.0) * 0.05
    flip_penalty = min(sign_flip_count / max(window_count - 1, 1), 1.0) * 0.15
    stability_score = max(
        0.0,
        min(
            1.0,
            pass_rate * 0.35
            + positive_rank_ic_ratio * 0.2
            + positive_spread_ratio * 0.15
            + rank_component * 0.15
            + spread_component * 0.15
            - flip_penalty
            - std_penalty,
        ),
    )

    return {
        "window_count": window_count,
        "pass_count": pass_count,
        "fail_count": window_count - pass_count,
        "pass_rate": round(pass_rate, 6),
        "positive_rank_ic_ratio": round(positive_rank_ic_ratio, 6),
        "positive_spread_ratio": round(positive_spread_ratio, 6),
        "sign_flip_count": sign_flip_count,
        "avg_rank_ic_mean": round(avg_rank_ic, 6),
        "avg_top_bottom_spread_mean": round(avg_spread, 6),
        "rank_ic_std": round(rank_ic_std, 6),
        "spread_std": round(spread_std, 6),
        "worst_rank_ic_mean": round(min(rank_ics), 6),
        "worst_top_bottom_spread_mean": round(min(spreads), 6),
        "stability_score": round(stability_score, 6),
        "pass_gate": pass_gate,
    }
