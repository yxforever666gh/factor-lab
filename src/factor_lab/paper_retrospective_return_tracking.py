from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PORTFOLIO_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_DATASET_PATH = ROOT / "artifacts" / "value_route_bucket_aware" / "runs" / "value_quality_no_distress_bucket_aware" / "dataset.csv"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "paper_portfolio" / "retrospective_return_tracking.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "paper_portfolio" / "retrospective_return_tracking.md"


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_dataset(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    frame = pd.read_csv(p)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    return frame


def _position_weights(portfolio: dict[str, Any]) -> dict[str, float]:
    positions = portfolio.get("positions") or []
    if not isinstance(positions, list) or not positions:
        return {}
    fallback_weight = 1.0 / len(positions)
    weights: dict[str, float] = {}
    for row in positions:
        if not isinstance(row, dict) or not row.get("ticker"):
            continue
        try:
            weight = float(row.get("weight", fallback_weight))
        except (TypeError, ValueError):
            weight = fallback_weight
        weights[str(row["ticker"])] = weight
    total = sum(weights.values())
    if total > 0:
        weights = {ticker: weight / total for ticker, weight in weights.items()}
    return weights


def compute_equal_weight_forward_return(
    portfolio: dict[str, Any],
    dataset: pd.DataFrame,
    return_column: str = "forward_return_5d",
) -> dict[str, Any]:
    weights = _position_weights(portfolio)
    position_count = len(weights)
    as_of_date = portfolio.get("as_of_date")

    if position_count == 0:
        return {
            "tracking_status": "insufficient_forward_window",
            "return_column": return_column,
            "portfolio_forward_return": None,
            "matched_position_count": 0,
            "missing_position_count": 0,
            "coverage": 0.0,
            "reason": "no_positions",
        }
    if dataset.empty or return_column not in dataset.columns or "ticker" not in dataset.columns:
        return {
            "tracking_status": "insufficient_forward_window",
            "return_column": return_column,
            "portfolio_forward_return": None,
            "matched_position_count": 0,
            "missing_position_count": position_count,
            "coverage": 0.0,
            "reason": "missing_return_column_or_dataset",
        }

    frame = dataset.copy()
    if as_of_date and "date" in frame.columns:
        date_value = pd.to_datetime(as_of_date)
        frame = frame[pd.to_datetime(frame["date"]) == date_value]
    returns = frame[["ticker", return_column]].copy()
    returns["ticker"] = returns["ticker"].astype(str)
    returns[return_column] = pd.to_numeric(returns[return_column], errors="coerce")
    returns = returns.dropna(subset=[return_column])
    latest_returns = returns.drop_duplicates(subset=["ticker"], keep="last").set_index("ticker")[return_column].to_dict()

    matched: list[tuple[str, float, float]] = []
    missing: list[str] = []
    for ticker, weight in weights.items():
        if ticker in latest_returns:
            matched.append((ticker, weight, float(latest_returns[ticker])))
        else:
            missing.append(ticker)

    if not matched:
        return {
            "tracking_status": "insufficient_forward_window",
            "return_column": return_column,
            "portfolio_forward_return": None,
            "matched_position_count": 0,
            "missing_position_count": len(missing),
            "coverage": 0.0,
            "reason": "no_matched_forward_returns",
            "missing_tickers_sample": missing[:10],
        }

    matched_weight = sum(weight for _, weight, _ in matched)
    portfolio_return = sum(weight * value for _, weight, value in matched)
    if matched_weight > 0:
        portfolio_return = portfolio_return / matched_weight

    return {
        "tracking_status": "ok",
        "return_column": return_column,
        "portfolio_forward_return": round(float(portfolio_return), 6),
        "matched_position_count": len(matched),
        "missing_position_count": len(missing),
        "coverage": round(len(matched) / position_count, 6),
        "matched_weight": round(float(matched_weight), 6),
        "missing_tickers_sample": missing[:10],
    }


def build_paper_retrospective_return_tracking(
    *,
    portfolio_path: str | Path = DEFAULT_PORTFOLIO_PATH,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    return_column: str = "forward_return_5d",
) -> dict[str, Any]:
    portfolio = load_json(portfolio_path)
    dataset = load_dataset(dataset_path)
    diagnostics = load_json(diagnostics_path)
    portfolio_return = compute_equal_weight_forward_return(portfolio, dataset, return_column=return_column)
    benchmark = (diagnostics.get("benchmark") or {}).copy()
    if not benchmark:
        benchmark = {"benchmark_id": None, "benchmark_name": None, "tracking_mode": "metadata_only"}
    benchmark.setdefault("tracking_mode", "metadata_only")

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tracking_status": portfolio_return.get("tracking_status"),
        "portfolio": {
            "strategy_name": portfolio.get("strategy_name"),
            "as_of_date": portfolio.get("as_of_date"),
            "position_count": portfolio.get("position_count", len(portfolio.get("positions") or [])),
        },
        "portfolio_return": portfolio_return,
        "benchmark": benchmark,
        "inputs": {
            "portfolio_path": str(portfolio_path),
            "dataset_path": str(dataset_path),
            "diagnostics_path": str(diagnostics_path),
            "return_column": return_column,
        },
    }


def retrospective_tracking_to_markdown(payload: dict[str, Any]) -> str:
    portfolio = payload.get("portfolio") or {}
    returns = payload.get("portfolio_return") or {}
    benchmark = payload.get("benchmark") or {}
    inputs = payload.get("inputs") or {}
    lines = [
        "# Paper Retrospective Return Tracking",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Tracking status: {payload.get('tracking_status')}",
        "",
        "## Portfolio",
        f"- Strategy: {portfolio.get('strategy_name')}",
        f"- As-of date: {portfolio.get('as_of_date')}",
        f"- Position count: {portfolio.get('position_count')}",
        "",
        "## Return tracking",
        f"- Return column: {returns.get('return_column')}",
        f"- Portfolio forward return: {returns.get('portfolio_forward_return')}",
        f"- Matched position count: {returns.get('matched_position_count')}",
        f"- Missing position count: {returns.get('missing_position_count')}",
        f"- Coverage: {returns.get('coverage')}",
        f"- Reason: {returns.get('reason')}",
        "",
        "## Benchmark",
        f"- Benchmark ID: {benchmark.get('benchmark_id')}",
        f"- Benchmark name: {benchmark.get('benchmark_name')}",
        f"- Tracking mode: {benchmark.get('tracking_mode')}",
        "",
        "## Inputs",
        f"- Portfolio path: {inputs.get('portfolio_path')}",
        f"- Dataset path: {inputs.get('dataset_path')}",
    ]
    return "\n".join(lines) + "\n"


def write_paper_retrospective_return_tracking(
    *,
    portfolio_path: str | Path = DEFAULT_PORTFOLIO_PATH,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    return_column: str = "forward_return_5d",
) -> dict[str, Any]:
    payload = build_paper_retrospective_return_tracking(
        portfolio_path=portfolio_path,
        dataset_path=dataset_path,
        diagnostics_path=diagnostics_path,
        return_column=return_column,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(retrospective_tracking_to_markdown(payload), encoding="utf-8")
    return payload
