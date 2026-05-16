from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.portfolio import build_composite_factor, evaluate_long_short_portfolio


_FORWARD_RETURN_CANDIDATES = [
    "forward_return_5d",
    "forward_return_10d",
    "forward_return_1d",
    "forward_return",
]


def _pick_forward_return_column(frame: pd.DataFrame) -> str | None:
    for column in _FORWARD_RETURN_CANDIDATES:
        if column in frame.columns:
            return column
    return None


def _select_top_basket(latest: pd.DataFrame, signal_col: str, long_q: float) -> list[str]:
    if latest.empty or signal_col not in latest.columns:
        return []
    cut = latest[signal_col].quantile(1 - long_q)
    selected = latest[latest[signal_col] >= cut].copy().sort_values(signal_col, ascending=False)
    return [str(row["ticker"]) for _, row in selected.iterrows() if row.get("ticker") is not None]


def _safe_corr(left: pd.Series, right: pd.Series) -> float | None:
    try:
        value = left.corr(right)
    except Exception:
        return None
    if pd.isna(value):
        return None
    return round(float(value), 6)


def _classify_contribution(delta_sharpe: float, delta_cost_adjusted_return: float) -> str:
    if delta_sharpe >= 0.05 or delta_cost_adjusted_return >= 0.02:
        return "positive"
    if delta_sharpe <= -0.05 or delta_cost_adjusted_return <= -0.02:
        return "negative"
    return "neutral"


def _empty_payload(strategy_name: str, reason: str) -> dict[str, Any]:
    return {
        "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
        "strategy_name": strategy_name,
        "status": "empty",
        "reason": reason,
        "forward_return_column": None,
        "full_portfolio": {},
        "rows": [],
    }


def build_portfolio_contribution_report(
    dataset_path: str | Path,
    factor_definitions: list[dict[str, Any]],
    output_path: str | Path,
    *,
    strategy_name: str = "paper_candidates_only",
    long_q: float = 0.2,
) -> dict[str, Any]:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not factor_definitions:
        payload = _empty_payload(strategy_name, "candidate_pool_empty")
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    frame = pd.read_csv(dataset_path)
    if frame.empty:
        payload = _empty_payload(strategy_name, "dataset_empty")
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    forward_return_column = _pick_forward_return_column(frame)
    if not forward_return_column:
        payload = _empty_payload(strategy_name, "forward_return_missing")
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    if forward_return_column != "forward_return_5d":
        frame["forward_return_5d"] = frame[forward_return_column]

    defs = [FactorDefinition(name=item["name"], expression=item["expression"]) for item in factor_definitions if item.get("name") and item.get("expression")]
    factor_weights = {item["name"]: float(item.get("weight_hint") or 1.0) for item in factor_definitions if item.get("name")}
    if not defs:
        payload = _empty_payload(strategy_name, "valid_factor_definitions_empty")
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    factor_value_cache = {definition.name: apply_factor(frame, definition) for definition in defs}
    full_signal = build_composite_factor(frame, defs, neutralize=False, factor_value_cache=factor_value_cache, factor_weights=factor_weights)
    full_eval = evaluate_long_short_portfolio(frame, full_signal, top_q=long_q, bottom_q=long_q)

    latest_date = frame["date"].max()
    latest = frame[frame["date"] == latest_date].copy()
    latest["full_signal"] = full_signal.loc[latest.index].values
    latest_full_basket = _select_top_basket(latest, "full_signal", long_q)

    rows: list[dict[str, Any]] = []
    for definition in defs:
        factor_values = factor_value_cache.get(definition.name)
        latest_factor_signal = pd.Series(factor_values.loc[latest.index].values, index=latest.index) if factor_values is not None else pd.Series(dtype=float)

        remaining_defs = [item for item in defs if item.name != definition.name]
        if remaining_defs:
            loo_weights = {name: weight for name, weight in factor_weights.items() if name != definition.name}
            loo_signal = build_composite_factor(frame, remaining_defs, neutralize=False, factor_value_cache=factor_value_cache, factor_weights=loo_weights)
            loo_eval = evaluate_long_short_portfolio(frame, loo_signal, top_q=long_q, bottom_q=long_q)
            latest["loo_signal"] = loo_signal.loc[latest.index].values
            latest_loo_basket = _select_top_basket(latest, "loo_signal", long_q)
        else:
            loo_eval = None
            latest_loo_basket = []

        overlap_ratio = None
        if latest_full_basket:
            overlap_ratio = round(
                len(set(latest_full_basket) & set(latest_loo_basket)) / max(len(set(latest_full_basket)), 1),
                6,
            ) if remaining_defs else 0.0

        delta_sharpe = float(full_eval.sharpe)
        delta_annual_return = float(full_eval.annual_return)
        delta_cost_adjusted_return = float(full_eval.cost_adjusted_annual_return)
        if loo_eval is not None:
            delta_sharpe -= float(loo_eval.sharpe)
            delta_annual_return -= float(loo_eval.annual_return)
            delta_cost_adjusted_return -= float(loo_eval.cost_adjusted_annual_return)

        latest_selected_mean = None
        if latest_full_basket and not latest_factor_signal.empty:
            selected_values = latest.loc[latest["ticker"].astype(str).isin(latest_full_basket)].copy()
            if not selected_values.empty:
                selected_values["factor_signal"] = latest_factor_signal.loc[selected_values.index].values
                latest_selected_mean = round(float(selected_values["factor_signal"].mean()), 6)

        rows.append(
            {
                "factor_name": definition.name,
                "expression": definition.expression,
                "weight_hint": factor_weights.get(definition.name),
                "contribution_class": _classify_contribution(delta_sharpe, delta_cost_adjusted_return),
                "delta_sharpe": round(delta_sharpe, 6),
                "delta_annual_return": round(delta_annual_return, 6),
                "delta_cost_adjusted_annual_return": round(delta_cost_adjusted_return, 6),
                "latest_overlap_ratio": overlap_ratio,
                "latest_selected_factor_mean": latest_selected_mean,
                "latest_signal_corr_with_full": _safe_corr(latest_factor_signal, latest["full_signal"] if "full_signal" in latest else pd.Series(dtype=float)),
            }
        )

    rows.sort(
        key=lambda row: (
            {"positive": 0, "neutral": 1, "negative": 2}.get(row.get("contribution_class") or "neutral", 9),
            -float(row.get("delta_sharpe") or 0.0),
            -float(row.get("delta_cost_adjusted_annual_return") or 0.0),
            row.get("factor_name") or "",
        )
    )

    payload = {
        "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
        "strategy_name": strategy_name,
        "status": "ok",
        "reason": None,
        "forward_return_column": forward_return_column,
        "full_portfolio": {
            **asdict(full_eval),
            "latest_date": str(latest_date.date()) if not pd.isna(latest_date) else None,
            "latest_long_basket": latest_full_basket,
        },
        "rows": rows,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
