from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.factors import FactorDefinition
from factor_lab.portfolio import build_composite_factor
from factor_lab.portfolio_contribution import build_portfolio_contribution_report


def resolve_latest_paper_portfolio_inputs(
    db_path: str | Path,
    *,
    fallback_candidate_pool_path: str | Path | None = None,
    fallback_dataset_path: str | Path | None = None,
) -> dict[str, Any]:
    db_path = Path(db_path)
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT run_id, created_at_utc, output_dir, config_path
            FROM workflow_runs
            WHERE status = 'finished'
            ORDER BY created_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    if row:
        run_id, created_at_utc, output_dir, config_path = row
        output_dir_path = Path(output_dir) if output_dir else None
        candidate_pool_path = output_dir_path / "candidate_pool.json" if output_dir_path else None
        dataset_path = output_dir_path / "dataset.csv" if output_dir_path else None
        if candidate_pool_path and dataset_path and candidate_pool_path.exists() and dataset_path.exists():
            return {
                "source": "latest_finished_run",
                "run_id": run_id,
                "created_at_utc": created_at_utc,
                "config_path": config_path,
                "output_dir": str(output_dir_path),
                "candidate_pool_path": candidate_pool_path,
                "dataset_path": dataset_path,
            }

    return {
        "source": "fallback",
        "run_id": None,
        "created_at_utc": None,
        "config_path": None,
        "output_dir": None,
        "candidate_pool_path": Path(fallback_candidate_pool_path) if fallback_candidate_pool_path else None,
        "dataset_path": Path(fallback_dataset_path) if fallback_dataset_path else None,
    }


def _empty_portfolio_payload(
    strategy_name: str,
    latest_date: pd.Timestamp | None,
    reason: str,
) -> dict[str, Any]:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy_name": strategy_name,
        "as_of_date": str(latest_date.date()) if latest_date is not None and not pd.isna(latest_date) else None,
        "position_count": 0,
        "positions": [],
        "reason": reason,
    }
    return payload


def build_paper_portfolio(
    dataset_path: str | Path,
    factor_definitions: list[dict[str, Any]],
    output_dir: str | Path,
    strategy_name: str = "paper_candidates_only",
    long_q: float = 0.2,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    frame = pd.read_csv(dataset_path)
    frame["date"] = pd.to_datetime(frame["date"])
    if frame.empty:
        payload = _empty_portfolio_payload(strategy_name, None, "dataset_empty")
        (output / "current_portfolio.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    latest_date = frame["date"].max()
    latest = frame[frame["date"] == latest_date].copy()

    if not factor_definitions:
        payload = _empty_portfolio_payload(strategy_name, latest_date, "candidate_pool_empty")
        payload["selected_factors"] = []
        payload["input_source"] = source_metadata or {}
        (output / "current_portfolio.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        build_portfolio_contribution_report(
            dataset_path=dataset_path,
            factor_definitions=[],
            output_path=output / "portfolio_contribution_report.json",
            strategy_name=strategy_name,
            long_q=long_q,
        )
        return payload

    defs = [FactorDefinition(name=item["name"], expression=item["expression"]) for item in factor_definitions]
    factor_weights = {
        item["name"]: float(item.get("allocated_weight") or item.get("weight_hint") or 1.0)
        for item in factor_definitions
        if item.get("name")
    }
    full_signal = build_composite_factor(frame, defs, neutralize=False, factor_weights=factor_weights)
    latest["signal"] = full_signal.loc[latest.index].values

    cut = latest["signal"].quantile(1 - long_q)
    target = latest[latest["signal"] >= cut].copy().sort_values("signal", ascending=False)
    if target.empty:
        payload = _empty_portfolio_payload(strategy_name, latest_date, "no_positions_selected")
        payload["selected_factors"] = [{"name": item["name"], "expression": item["expression"], "weight_hint": item.get("weight_hint"), "allocated_weight": item.get("allocated_weight"), "portfolio_weight_target": item.get("portfolio_weight_target"), "portfolio_bucket": item.get("portfolio_bucket"), "portfolio_bucket_label": item.get("portfolio_bucket_label"), "approval_tier": item.get("approval_tier"), "lifecycle_state": item.get("lifecycle_state") or item.get("universe_state"), "governance_action": item.get("governance_action"), "max_weight": item.get("max_weight"), "family_budget_cap": item.get("family_budget_cap"), "bucket_budget_cap": item.get("bucket_budget_cap"), "budget_reason": item.get("budget_reason")} for item in factor_definitions if item.get("name") and item.get("expression")]
        payload["input_source"] = source_metadata or {}
        (output / "current_portfolio.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        build_portfolio_contribution_report(
            dataset_path=dataset_path,
            factor_definitions=factor_definitions,
            output_path=output / "portfolio_contribution_report.json",
            strategy_name=strategy_name,
            long_q=long_q,
        )
        return payload

    target["weight"] = 1.0 / len(target)
    positions = [
        {
            "ticker": row["ticker"],
            "signal": round(float(row["signal"]), 6),
            "weight": round(float(row["weight"]), 6),
        }
        for _, row in target.iterrows()
    ]

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy_name": strategy_name,
        "as_of_date": str(latest_date.date()),
        "position_count": len(positions),
        "positions": positions,
        "selected_factors": [{"name": item["name"], "expression": item["expression"], "weight_hint": item.get("weight_hint"), "allocated_weight": item.get("allocated_weight"), "portfolio_weight_target": item.get("portfolio_weight_target"), "portfolio_bucket": item.get("portfolio_bucket"), "portfolio_bucket_label": item.get("portfolio_bucket_label"), "approval_tier": item.get("approval_tier"), "lifecycle_state": item.get("lifecycle_state") or item.get("universe_state"), "governance_action": item.get("governance_action"), "max_weight": item.get("max_weight"), "family_budget_cap": item.get("family_budget_cap"), "bucket_budget_cap": item.get("bucket_budget_cap"), "budget_reason": item.get("budget_reason")} for item in factor_definitions if item.get("name") and item.get("expression")],
        "input_source": source_metadata or {},
    }
    (output / "current_portfolio.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    build_portfolio_contribution_report(
        dataset_path=dataset_path,
        factor_definitions=factor_definitions,
        output_path=output / "portfolio_contribution_report.json",
        strategy_name=strategy_name,
        long_q=long_q,
    )
    return payload


def append_portfolio_history(current_path: str | Path, history_path: str | Path) -> list[dict[str, Any]]:
    current = json.loads(Path(current_path).read_text(encoding="utf-8"))
    path = Path(history_path)
    history = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    history.append(current)
    path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    return history


def build_portfolio_change_log(current_path: str | Path, history_path: str | Path, output_path: str | Path) -> str:
    current = json.loads(Path(current_path).read_text(encoding="utf-8"))
    history = json.loads(Path(history_path).read_text(encoding="utf-8")) if Path(history_path).exists() else []
    previous = history[-2] if len(history) >= 2 else None

    current_set = {p["ticker"] for p in current.get("positions", [])}
    previous_set = {p["ticker"] for p in (previous.get("positions", []) if previous else [])}

    added = sorted(current_set - previous_set)
    removed = sorted(previous_set - current_set)

    lines = [
        "# 纸面组合变更",
        "",
        f"- 最新日期：{current.get('as_of_date', '-')}",
        f"- 策略：{current.get('strategy_name', '-')}",
        f"- 当前持仓数：{current.get('position_count', 0)}",
        f"- 新增持仓：{', '.join(added) if added else '无'}",
        f"- 移除持仓：{', '.join(removed) if removed else '无'}",
    ]
    text = "\n".join(lines)
    Path(output_path).write_text(text, encoding="utf-8")
    return text
