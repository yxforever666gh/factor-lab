"""Deterministic negative and reverse-direction controls for factor research."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable, Mapping

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class NegativeControlMetric:
    control_name: str
    metric: float
    passed_promotion_gate: bool = False

    def __post_init__(self) -> None:
        if not self.control_name.strip():
            raise ValueError("control_name must not be empty")
        if not isfinite(float(self.metric)):
            raise ValueError("negative-control metric must be finite")


@dataclass(frozen=True)
class NegativeControlDecision:
    passed: bool
    pass_count: int
    control_count: int
    pass_rate: float
    strongest_control_metric: float | None
    reasons: tuple[str, ...]


def generate_negative_control_signals(
    frame: pd.DataFrame,
    signal: pd.Series | Iterable[float],
    *,
    entity_column: str = "ticker",
    time_column: str = "date",
    seed: int = 0,
    minimum_time_shift: int = 20,
) -> dict[str, pd.Series]:
    """Create controls that preserve signal marginals but break return alignment.

    The circular control is diagnostic-only and may contain values from later
    dates by construction.  It is never a valid factor and must only be run in
    the explicitly marked negative-control path.
    """

    missing = {entity_column, time_column}.difference(frame.columns)
    if missing:
        raise KeyError(f"missing negative-control columns: {sorted(missing)}")
    if isinstance(signal, pd.Series):
        if len(signal) != len(frame) or not signal.index.equals(frame.index):
            raise ValueError("signal index and length must match frame")
        source = pd.to_numeric(signal, errors="coerce").astype(float).rename("signal")
    else:
        signal_values = list(signal)
        if len(signal_values) != len(frame):
            raise ValueError("signal length must match frame")
        source = pd.Series(signal_values, index=frame.index, dtype=float, name="signal")
    rng = np.random.default_rng(seed)

    cross_section = source.copy()
    time_values = pd.to_datetime(frame[time_column], errors="raise")
    position_frame = pd.DataFrame({"position": np.arange(len(frame)), "time": time_values.to_numpy()})
    for _, positions in position_frame.groupby("time", sort=False):
        loc = positions["position"].to_numpy(dtype=int)
        shuffled = source.iloc[loc].to_numpy(copy=True)
        rng.shuffle(shuffled)
        cross_section.iloc[loc] = shuffled

    circular = source.copy()
    entity_values = frame[entity_column].astype("string")
    order = pd.DataFrame(
        {
            "position": np.arange(len(frame)),
            "entity": entity_values.to_numpy(),
            "time": time_values.to_numpy(),
        }
    ).sort_values(["entity", "time", "position"], kind="stable")
    for _, group in order.groupby("entity", sort=False):
        loc = group["position"].to_numpy(dtype=int)
        if len(loc) <= 1:
            circular.iloc[loc] = np.nan
            continue
        lower = min(max(minimum_time_shift, 1), len(loc) - 1)
        eligible = np.arange(lower, len(loc), dtype=int)
        if not len(eligible):
            eligible = np.arange(1, len(loc), dtype=int)
        shift = int(rng.choice(eligible))
        circular.iloc[loc] = np.roll(source.iloc[loc].to_numpy(), shift)

    random_rank = pd.Series(np.nan, index=frame.index, dtype=float)
    for _, positions in position_frame.groupby("time", sort=False):
        loc = positions["position"].to_numpy(dtype=int)
        random_rank.iloc[loc] = rng.permutation(np.arange(1, len(loc) + 1)) / max(len(loc), 1)

    return {
        "cross_section_permutation": cross_section.rename("cross_section_permutation"),
        "circular_entity_time_shift": circular.rename("circular_entity_time_shift"),
        "random_cross_section_rank": random_rank.rename("random_cross_section_rank"),
    }


def reverse_direction_signal(signal: pd.Series | Iterable[float]) -> pd.Series:
    source = signal.copy() if isinstance(signal, pd.Series) else pd.Series(signal, dtype=float)
    return (-pd.to_numeric(source, errors="coerce")).rename("reverse_direction")


def evaluate_negative_control_gate(
    results: Iterable[NegativeControlMetric | Mapping[str, object]],
    *,
    maximum_pass_rate: float = 0.0,
    minimum_control_count: int = 2,
) -> NegativeControlDecision:
    if not 0 <= maximum_pass_rate <= 1:
        raise ValueError("maximum_pass_rate must be in [0, 1]")
    if minimum_control_count < 1:
        raise ValueError("minimum_control_count must be positive")
    rows: list[NegativeControlMetric] = []
    for result in results:
        if isinstance(result, NegativeControlMetric):
            rows.append(result)
        else:
            rows.append(
                NegativeControlMetric(
                    control_name=str(result.get("control_name") or result.get("name") or "unknown"),
                    metric=float(result.get("metric") or 0.0),
                    passed_promotion_gate=bool(result.get("passed_promotion_gate") or result.get("passed")),
                )
            )
    if not rows:
        return NegativeControlDecision(
            passed=False,
            pass_count=0,
            control_count=0,
            pass_rate=1.0,
            strongest_control_metric=None,
            reasons=("negative_controls_missing",),
        )
    pass_count = sum(item.passed_promotion_gate for item in rows)
    pass_rate = pass_count / len(rows)
    reasons_list: list[str] = []
    if len(rows) < minimum_control_count:
        reasons_list.append("insufficient_negative_controls")
    if len({item.control_name for item in rows}) != len(rows):
        reasons_list.append("duplicate_negative_controls")
    if pass_rate > maximum_pass_rate:
        reasons_list.append("negative_control_pass_rate_exceeded")
    reasons = tuple(reasons_list)
    return NegativeControlDecision(
        passed=not reasons,
        pass_count=pass_count,
        control_count=len(rows),
        pass_rate=float(pass_rate),
        strongest_control_metric=max(float(item.metric) for item in rows),
        reasons=reasons,
    )
