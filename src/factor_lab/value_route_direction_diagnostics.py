from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def load_run_metrics(run_dir: str | Path, *, route_id: str, direction: str) -> dict[str, Any]:
    run = Path(run_dir)
    results_path = run / "results.json"
    scores_path = run / "factor_scores.json"
    results = json.loads(results_path.read_text(encoding="utf-8")) if results_path.exists() else []
    row = results[0] if isinstance(results, list) and results else (results if isinstance(results, dict) else {})
    scores = json.loads(scores_path.read_text(encoding="utf-8")) if scores_path.exists() else []
    score_row = scores[0] if isinstance(scores, list) and scores else (scores if isinstance(scores, dict) else {})
    return {
        "route_id": route_id,
        "direction": direction,
        "factor_name": row.get("factor_name") or score_row.get("factor_name"),
        "expression": row.get("expression"),
        "rank_ic_mean": _safe_float(row.get("rank_ic_mean")),
        "rank_ic_ir": _safe_float(row.get("rank_ic_ir")),
        "top_bottom_spread_mean": _safe_float(row.get("top_bottom_spread_mean")),
        "bottom_top_spread_mean": -_safe_float(row.get("top_bottom_spread_mean")) if _safe_float(row.get("top_bottom_spread_mean")) is not None else None,
        "pass_gate": bool(row.get("pass_gate")) if "pass_gate" in row else None,
        "fail_reason": row.get("fail_reason"),
        "score": _safe_float(score_row.get("score")),
        "run_dir": str(run),
    }


def diagnose_metric_direction(pair: dict[str, Any]) -> dict[str, Any]:
    original = pair.get("original") or {}
    inverted = pair.get("inverted") or {}
    o_ic = _safe_float(original.get("rank_ic_mean"))
    o_spread = _safe_float(original.get("top_bottom_spread_mean"))
    i_ic = _safe_float(inverted.get("rank_ic_mean"))
    i_spread = _safe_float(inverted.get("top_bottom_spread_mean"))
    reasons: list[str] = []
    if o_ic is not None and o_ic > 0 and o_spread is not None and o_spread < 0:
        reasons.append("original_positive_ic_negative_spread")
    if i_spread is not None and i_spread > 0:
        reasons.append("inverted_spread_positive")
    if o_spread is not None and i_spread is not None and o_spread < 0 and i_spread < 0:
        reasons.append("both_directions_negative_spread")
    if o_spread is not None and i_spread is not None and abs(o_spread + i_spread) < max(1e-12, abs(o_spread) * 0.05):
        reasons.append("spread_is_antisymmetric_under_inversion")

    if "original_positive_ic_negative_spread" in reasons and "inverted_spread_positive" in reasons:
        recommendation = "invert_signal_or_portfolio_direction_check"
    elif "both_directions_negative_spread" in reasons:
        recommendation = "mechanism_or_portfolio_construction_review"
    elif o_spread is not None and o_spread > 0:
        recommendation = "keep_original_direction"
    elif i_spread is not None and i_spread > 0:
        recommendation = "prefer_inverted_direction"
    else:
        recommendation = "insufficient_direction_evidence"

    return {
        "route_id": pair.get("route_id"),
        "factor_name": pair.get("factor_name") or original.get("factor_name") or inverted.get("factor_name"),
        "original_rank_ic_mean": o_ic,
        "original_spread_mean": o_spread,
        "inverted_rank_ic_mean": i_ic,
        "inverted_spread_mean": i_spread,
        "bottom_top_spread_mean": -o_spread if o_spread is not None else None,
        "recommendation": recommendation,
        "reasons": reasons,
        "original": original,
        "inverted": inverted,
    }


def pair_metrics(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pairs: dict[str, dict[str, Any]] = {}
    for row in metrics:
        route = str(row.get("route_id") or "unknown")
        pair = pairs.setdefault(route, {"route_id": route})
        pair[str(row.get("direction") or "unknown")] = row
        pair.setdefault("factor_name", row.get("factor_name"))
    return [diagnose_metric_direction(pair) for pair in pairs.values()]


def markdown_report(diagnostics: list[dict[str, Any]]) -> str:
    lines = [
        "# Value Route Direction Diagnostics",
        "",
        "## Executive conclusion",
    ]
    recs = [d.get("recommendation") for d in diagnostics]
    if any(r in {"invert_signal_or_portfolio_direction_check", "prefer_inverted_direction"} for r in recs):
        lines.append("At least one route improves when inverted; inspect factor direction and portfolio convention before restarting daemon.")
    elif diagnostics:
        lines.append("No route has passed direction diagnostics yet; keep daemon paused and review mechanism/data/portfolio construction.")
    else:
        lines.append("No diagnostics available.")
    lines.extend(["", "## Original vs inverted", "", "| route | original IC | original spread | inverted IC | inverted spread | recommendation |", "|---|---:|---:|---:|---:|---|"])
    for d in diagnostics:
        lines.append(
            f"| {d.get('route_id')} | {d.get('original_rank_ic_mean')} | {d.get('original_spread_mean')} | "
            f"{d.get('inverted_rank_ic_mean')} | {d.get('inverted_spread_mean')} | {d.get('recommendation')} |"
        )
    lines.extend(["", "## Portfolio convention verdict", "", "Portfolio convention must be verified by `tests/test_portfolio_direction_convention.py`; do not restore daemon if this test fails."])
    lines.extend(["", "## Daemon restoration", "", "Daemon may only be restored after admission integration and controlled restart dry-run pass."])
    return "\n".join(lines) + "\n"


def write_direction_diagnostics(pairs: list[dict[str, Any]], *, output_dir: str | Path) -> dict[str, Any]:
    diagnostics = [diagnose_metric_direction(pair) for pair in pairs]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "direction_diagnostics.json"
    md_path = out / "direction_diagnostics.md"
    payload = {"diagnostics": diagnostics, "summary": {"route_count": len(diagnostics)}}
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(markdown_report(diagnostics), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path), "diagnostics": diagnostics}


def write_direction_diagnostics_from_runs(*, runs_dir: str | Path, output_dir: str | Path) -> dict[str, Any]:
    metrics: list[dict[str, Any]] = []
    for run in sorted(Path(runs_dir).glob("*")):
        if not run.is_dir():
            continue
        name = run.name
        direction = "inverted" if name.endswith("_inverted") else "original"
        route = name[: -len("_inverted")] if direction == "inverted" else name[: -len("_original")] if name.endswith("_original") else name
        metrics.append(load_run_metrics(run, route_id=route, direction=direction))
    diagnostics = pair_metrics(metrics)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "direction_diagnostics.json"
    md_path = out / "direction_diagnostics.md"
    payload = {"metrics": metrics, "diagnostics": diagnostics, "summary": {"route_count": len(diagnostics)}}
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(markdown_report(diagnostics), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path), "diagnostics": diagnostics}
