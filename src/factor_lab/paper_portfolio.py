from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.factors import FactorDefinition
from factor_lab.portfolio import build_composite_factor


def build_paper_portfolio(
    dataset_path: str | Path,
    factor_definitions: list[dict[str, Any]],
    output_dir: str | Path,
    strategy_name: str = "paper_candidates_only",
    long_q: float = 0.2,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    frame = pd.read_csv(dataset_path)
    frame["date"] = pd.to_datetime(frame["date"])
    latest_date = frame["date"].max()
    latest = frame[frame["date"] == latest_date].copy()

    defs = [FactorDefinition(name=item["name"], expression=item["expression"]) for item in factor_definitions]
    full_signal = build_composite_factor(frame, defs, neutralize=False)
    latest["signal"] = full_signal.loc[latest.index].values

    cut = latest["signal"].quantile(1 - long_q)
    target = latest[latest["signal"] >= cut].copy().sort_values("signal", ascending=False)
    if target.empty:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "strategy_name": strategy_name,
            "as_of_date": str(latest_date.date()),
            "positions": [],
        }
        (output / "current_portfolio.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
    }
    (output / "current_portfolio.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
