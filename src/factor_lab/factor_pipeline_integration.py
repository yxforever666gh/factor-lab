from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.evaluation import evaluate_factor
from factor_lab.portfolio_optimizer import PortfolioOptimizer, compare_optimization_methods
from factor_lab.capacity_analysis import CapacityAnalyzer, create_capacity_report
from factor_lab.factor_attribution import FactorAttribution, create_style_factors, create_attribution_report
from factor_lab.factor_monitoring import FactorMonitor, create_monitoring_report
from factor_lab.backtest_validation import validate_factor_backtest, create_validation_report


def _json_default(obj: Any):
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _detect_return_column(frame: pd.DataFrame) -> str | None:
    for col in ("forward_return_5d", "return", "returns"):
        if col in frame.columns:
            return col
    return None


def _load_dataset(dataset_path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(dataset_path)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    return frame


def _long_short_return_series(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    return_col: str,
    top_q: float = 0.2,
    bottom_q: float = 0.2,
) -> tuple[pd.Series, float]:
    work = frame[["date", "ticker", return_col]].copy()
    work["signal"] = signal.values
    work = work.dropna(subset=["signal", return_col]).copy()
    if work.empty:
        return pd.Series(dtype=float), 0.0

    group_sizes = work.groupby("date")["signal"].transform("size")
    work = work[group_sizes >= 10].copy()
    if work.empty:
        return pd.Series(dtype=float), 0.0

    group_sizes = work.groupby("date")["signal"].transform("size")
    long_n = group_sizes.apply(lambda n: max(1, int(np.ceil(n * top_q))))
    short_n = group_sizes.apply(lambda n: max(1, int(np.ceil(n * bottom_q))))
    long_rank = work.groupby("date")["signal"].rank(method="first", ascending=False)
    short_rank = work.groupby("date")["signal"].rank(method="first", ascending=True)

    long_mask = long_rank <= long_n
    short_mask = short_rank <= short_n
    if not long_mask.any() or not short_mask.any():
        return pd.Series(dtype=float), 0.0

    long_counts = long_mask.groupby(work["date"]).transform("sum")
    short_counts = short_mask.groupby(work["date"]).transform("sum")
    work["weight"] = 0.0
    work.loc[long_mask, "weight"] = 1.0 / long_counts[long_mask]
    work.loc[short_mask, "weight"] = -1.0 / short_counts[short_mask]

    series = (work["weight"] * work[return_col]).groupby(work["date"]).sum().sort_index()

    turnovers: list[float] = []
    prev_weights = None
    for _, group in work.loc[work["weight"] != 0.0].groupby("date", sort=True):
        weights = group.set_index("ticker")["weight"]
        if prev_weights is not None:
            all_idx = weights.index.union(prev_weights.index)
            turnover = (weights.reindex(all_idx, fill_value=0.0) - prev_weights.reindex(all_idx, fill_value=0.0)).abs().sum() / 2.0
            turnovers.append(float(turnover))
        prev_weights = weights
    avg_turnover = float(pd.Series(turnovers).mean()) if turnovers else 0.0
    return series, avg_turnover


def _latest_market_slice(frame: pd.DataFrame) -> pd.DataFrame:
    latest_date = frame["date"].max()
    latest = frame[frame["date"] == latest_date].copy()

    amount_col = None
    for col in ("amount_20d_avg", "amount_20d", "amount"):
        if col in latest.columns:
            amount_col = col
            break
    market_cap_col = "market_cap" if "market_cap" in latest.columns else ("total_mv" if "total_mv" in latest.columns else None)
    price_col = "price" if "price" in latest.columns else ("close" if "close" in latest.columns else None)

    payload = pd.DataFrame(index=latest["ticker"])
    payload["amount_20d_avg"] = latest[amount_col].values if amount_col else 1e8
    if market_cap_col:
        payload["market_cap"] = latest[market_cap_col].values
    if price_col:
        payload["price"] = latest[price_col].values
    return payload


def _build_style_input(frame: pd.DataFrame, return_col: str) -> pd.DataFrame:
    market_data = frame.copy()
    if return_col != "return":
        market_data["return"] = market_data[return_col]
    if "market_cap" not in market_data.columns and "total_mv" in market_data.columns:
        market_data["market_cap"] = market_data["total_mv"]
    if "momentum_20d" not in market_data.columns and {"close", "close_20d"}.issubset(market_data.columns):
        market_data["momentum_20d"] = market_data["close"] / market_data["close_20d"] - 1.0
    return market_data


def build_integrated_factor_reports(
    *,
    approved_universe: dict[str, Any],
    recent_artifacts: list[dict[str, Any]],
    artifacts_dir: str | Path,
    validation_simulations: int = 200,
) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    latest_artifact = next(
        (artifact for artifact in recent_artifacts if artifact.get("dataset_path") and Path(artifact["dataset_path"]).exists()),
        None,
    )
    rows = [row for row in (approved_universe.get("rows") or []) if row.get("factor_name") and row.get("expression")]

    if not latest_artifact or not rows:
        payload = {
            "available": False,
            "reason": "missing_dataset_or_approved_rows",
            "optimization": {},
            "capacity": [],
            "attribution": [],
            "monitoring": [],
            "validation": [],
        }
        (artifacts_dir / "integrated_factor_reports.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        return payload

    frame = _load_dataset(latest_artifact["dataset_path"])
    return_col = _detect_return_column(frame)
    if not return_col or not {"date", "ticker"}.issubset(frame.columns):
        payload = {
            "available": False,
            "reason": "dataset_missing_required_columns",
            "optimization": {},
            "capacity": [],
            "attribution": [],
            "monitoring": [],
            "validation": [],
        }
        (artifacts_dir / "integrated_factor_reports.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        return payload

    thresholds = {"min_rank_ic": 0.03, "min_top_bottom_spread": 0.0, "min_sharpe_net": 1.0}
    latest_market = _latest_market_slice(frame)
    style_returns = create_style_factors(_build_style_input(frame, return_col))
    attributor = FactorAttribution()
    capacity_analyzer = CapacityAnalyzer()
    monitor = FactorMonitor()

    factor_return_series: dict[str, pd.Series] = {}
    capacity_reports: list[dict[str, Any]] = []
    attribution_reports: list[dict[str, Any]] = []
    validation_reports: list[dict[str, Any]] = []
    evaluation_reports: list[dict[str, Any]] = []
    per_factor_turnover: dict[str, float] = {}
    per_factor_ic: dict[str, float] = {}

    for row in rows:
        name = row["factor_name"]
        definition = FactorDefinition(name=name, expression=row["expression"])
        signal = apply_factor(frame, definition)

        factor_frame = frame[["date", "ticker", return_col]].copy()
        factor_frame = factor_frame.rename(columns={return_col: "forward_return_5d"})
        factor_frame["factor_value"] = signal.values
        evaluation = evaluate_factor(factor_frame, name, definition.expression, thresholds)
        evaluation_reports.append(evaluation.to_dict())
        per_factor_ic[name] = float(evaluation.rank_ic_mean)
        per_factor_turnover[name] = float(evaluation.turnover_rate)

        returns_series, avg_turnover = _long_short_return_series(frame, signal, return_col=return_col)
        factor_return_series[name] = returns_series
        if avg_turnover > 0:
            per_factor_turnover[name] = avg_turnover

        latest_signal = pd.Series(signal.loc[frame["date"] == frame["date"].max()].values, index=latest_market.index[: len(signal.loc[frame["date"] == frame["date"].max()])])
        latest_signal = latest_signal.reindex(latest_market.index)
        capacity = capacity_analyzer.estimate_capacity(latest_signal, latest_market)
        capacity_reports.append(create_capacity_report(name, capacity))

        if not returns_series.empty and not style_returns.empty:
            aligned = returns_series.reindex(style_returns.index).dropna()
            aligned_styles = style_returns.reindex(aligned.index).dropna()
            aligned = aligned.reindex(aligned_styles.index)
            attribution = attributor.attribute_to_styles(aligned, aligned_styles)
            attribution_reports.append(create_attribution_report(name, attribution))
        else:
            attribution_reports.append(create_attribution_report(name, attributor._empty_result()))

        validation = validate_factor_backtest(returns_series, n_simulations=validation_simulations) if not returns_series.empty else {
            "decision": {"passed": False, "observed_sharpe": 0.0, "sample_out_decay": 1.0, "p_value": 1.0, "rolling_window_passed": False, "monte_carlo_passed": False, "reason": "empty_return_series"},
            "rolling_window": {"window_count": 0, "average_train_sharpe": 0.0, "average_test_sharpe": 0.0},
            "monte_carlo": {"null_mean_sharpe": 0.0, "null_std_sharpe": 0.0},
        }
        validation_reports.append(create_validation_report(name, validation))

    factor_returns_df = pd.concat(factor_return_series, axis=1).dropna(how="all") if factor_return_series else pd.DataFrame()

    optimization_payload: dict[str, Any] = {}
    monitoring_reports: list[dict[str, Any]] = []
    if not factor_returns_df.empty and len(factor_returns_df.columns) >= 2:
        optimizer = PortfolioOptimizer()
        optimization_payload = optimizer.optimize_weights(factor_returns_df)
        optimization_payload["method_comparison"] = compare_optimization_methods(factor_returns_df).to_dict(orient="records")

        midpoint = max(1, len(factor_returns_df) // 2)
        baseline_df = factor_returns_df.iloc[:midpoint]
        current_df = factor_returns_df.iloc[midpoint:]
        baseline_corr = baseline_df.corr() if len(baseline_df.columns) >= 2 else pd.DataFrame()
        current_corr = current_df.corr() if len(current_df.columns) >= 2 else pd.DataFrame()

        for factor_name in factor_returns_df.columns:
            baseline_series = baseline_df[factor_name].dropna()
            current_series = current_df[factor_name].dropna()
            baseline_metrics = {
                "ic": per_factor_ic.get(factor_name, 0.0),
                "turnover": per_factor_turnover.get(factor_name, 0.0),
                "max_drawdown": float(abs(((1 + baseline_series.fillna(0)).cumprod() / (1 + baseline_series.fillna(0)).cumprod().cummax() - 1).min())) if not baseline_series.empty else 0.0,
                "correlation_matrix": baseline_corr,
            }
            current_metrics = {
                "ic": per_factor_ic.get(factor_name, 0.0) * (0.9 if len(current_series) >= 10 else 1.0),
                "turnover": per_factor_turnover.get(factor_name, 0.0),
                "returns": current_series,
                "correlation_matrix": current_corr,
            }
            monitoring_reports.append(create_monitoring_report(monitor.monitor_factor_health(factor_name, current_metrics, baseline_metrics)))

    payload = {
        "available": True,
        "source_run_id": latest_artifact.get("run", {}).get("run_id"),
        "dataset_path": str(latest_artifact.get("dataset_path")),
        "approved_factor_count": len(rows),
        "optimization": optimization_payload,
        "capacity": capacity_reports,
        "attribution": attribution_reports,
        "monitoring": monitoring_reports,
        "validation": validation_reports,
        "evaluation": evaluation_reports,
    }

    (artifacts_dir / "integrated_factor_reports.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    (artifacts_dir / "capacity_reports.json").write_text(json.dumps(capacity_reports, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    (artifacts_dir / "attribution_reports.json").write_text(json.dumps(attribution_reports, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    (artifacts_dir / "monitoring_reports.json").write_text(json.dumps(monitoring_reports, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    (artifacts_dir / "validation_reports.json").write_text(json.dumps(validation_reports, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    return payload
